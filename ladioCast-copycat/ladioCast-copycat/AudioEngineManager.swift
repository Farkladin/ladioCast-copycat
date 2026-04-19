//
//  AudioEngineManager.swift
//  ladioCast-copycat
//
//  Real-time audio router built on top of AVAudioEngine.
//
//  ──────────────────────────────────────────────────────────────────────
//  ARCHITECTURE
//  ──────────────────────────────────────────────────────────────────────
//
//  macOS AVAudioEngine exposes `inputNode` and `outputNode`. In a single
//  engine these are backed by a shared duplex AUHAL unit, so
//  `kAudioOutputUnitProperty_CurrentDevice` can only hold ONE device at
//  a time. Calling it twice (once for input, once for output) silently
//  overwrites the first value and playback falls back to the system
//  default output — exactly the symptom of "I hear my input on the
//  speakers but nothing reaches BlackHole".
//
//  To route between two DIFFERENT devices we run two independent
//  AVAudioEngine instances, each owning its own AUHAL:
//
//     ┌─────────────────────┐              ┌───────────────────────┐
//     │   inputEngine       │              │   outputEngine        │
//     │                     │              │                       │
//     │   inputNode ──▶ Sink│              │ Source ──▶ Mixer ──▶ ─│─▶ device
//     │  (AUHAL=inDev)      │              │  (reads)  (volume)    │
//     └─────────┬───────────┘              └──────────▲────────────┘
//               │                                     │
//               │        shared FloatRingBuffer       │
//               └──────────────────────────────────── ┘
//
//  The sink callback (input engine, audio thread) picks the selected
//  device channels, sample-rate converts them to the output engine's
//  native rate, and writes them into the ring buffer. The source
//  callback (output engine, audio thread) reads from the ring buffer
//  and fans each bus channel out to the user-selected device output
//  channel, leaving unselected device channels silent.
//
//  ──────────────────────────────────────────────────────────────────────
//  WHY NO AUHAL CHANNEL MAP
//  ──────────────────────────────────────────────────────────────────────
//
//  AVAudioEngine's wrapped I/O nodes don't reliably accept
//  `kAudioOutputUnitProperty_ChannelMap` and return -10877
//  (kAudioUnitErr_InvalidElement). We do channel selection manually in
//  Swift inside the sink/source callbacks instead — same result, no
//  CoreAudio complaints.
//

import Foundation
import Combine
import AVFoundation
import CoreAudio
import AudioToolbox
import os
import os.log

private let logger = Logger(
    subsystem: Bundle.main.bundleIdentifier ?? "ladioCast-copycat",
    category: "AudioEngineManager")

// MARK: - AudioEngineManager

/// Coordinates device selection, engine lifecycle and the two-engine
/// audio routing graph. All public API is main-actor isolated; the
/// realtime audio-thread work lives inside `InputPipeline` /
/// `OutputPipeline` so the main actor never touches mutable state that
/// the audio thread is reading.
@MainActor
final class AudioEngineManager: ObservableObject {

    // MARK: Published UI state

    /// CoreAudio device to capture from. Changes take effect on the
    /// next `start()` call.
    @Published var selectedInput: AudioDevice?

    /// CoreAudio device to render to. Changes take effect on the next
    /// `start()` call.
    @Published var selectedOutput: AudioDevice?

    /// Ordered list of 0-based input-device channel indices. The k-th
    /// entry feeds engine bus channel k. Example: `[0, 1]` reads device
    /// channels 1 & 2 into engine L/R.
    @Published var inputChannelSelection: [Int] = [0, 1]

    /// Ordered list of 0-based output-device channel indices. The k-th
    /// entry receives engine bus channel k. Example: `[2, 3]` routes
    /// engine L/R to device output channels 3 & 4.
    @Published var outputChannelSelection: [Int] = [0, 1]

    /// Master output gain, 0.0 – 1.0. Applied on the output mixer.
    @Published var volume: Float = 1.0 {
        didSet { outputMixer.outputVolume = volume }
    }

    /// True while both engines are running and audio is flowing.
    @Published private(set) var isRunning = false

    /// Most recent user-facing error message, or `nil` on success.
    @Published private(set) var lastError: String?

    // MARK: Engines and nodes

    private let inputEngine  = AVAudioEngine()
    private let outputEngine = AVAudioEngine()
    private let outputMixer  = AVAudioMixerNode()

    /// Holds the audio-thread state for the currently running input
    /// engine. Nil when stopped. Captured by the sink callback.
    private var inputPipeline: InputPipeline?

    /// Holds the audio-thread state for the currently running output
    /// engine. Nil when stopped. Captured by the source-node callback.
    private var outputPipeline: OutputPipeline?

    /// Shared SPSC ring buffer: producer = input sink, consumer = output
    /// source-node render block. Channel count is reconfigured each
    /// `start()` to `min(inputSel.count, outputSel.count)`.
    private let ringBuffer = FloatRingBuffer(frameCapacity: 32_768,
                                             initialChannels: 2)

    // MARK: Config-change handling

    private var inputConfigObserver:  NSObjectProtocol?
    private var outputConfigObserver: NSObjectProtocol?

    private var isReconfiguring = false
    private var reconfigureTask: Task<Void, Never>?

    /// How many consecutive rebuild attempts we have made since the
    /// last successful `start`. Resets on success; stops permanently
    /// once it exceeds `maxRetries`.
    private var retryCount = 0
    private static let maxRetries = 3

    /// Monotonic deadline during which `AVAudioEngineConfigurationChange`
    /// notifications are ignored. Without this, our own calls to
    /// `AudioUnitSetProperty(…, kAudioOutputUnitProperty_CurrentDevice)`
    /// fire a notification that reenters the rebuild path, producing an
    /// infinite loop.
    private var cooldownUntil: Date?
    private static let startupCooldown: TimeInterval = 2.0

    // MARK: Lifecycle

    init() {
        outputEngine.attach(outputMixer)
    }

    deinit {
        if let o = inputConfigObserver  { NotificationCenter.default.removeObserver(o) }
        if let o = outputConfigObserver { NotificationCenter.default.removeObserver(o) }
    }

    // MARK: - Microphone permission

    /// Requests microphone access. Required on every macOS version for
    /// any non-trivial capture (including virtual / loopback devices
    /// like BlackHole). Returns `true` if already granted or newly
    /// granted.
    func requestMicPermission() async -> Bool {
        switch AVCaptureDevice.authorizationStatus(for: .audio) {
        case .authorized:    return true
        case .notDetermined: return await AVCaptureDevice.requestAccess(for: .audio)
        default:             return false
        }
    }

    // MARK: - Public start / stop

    /// Tears down any running graph and brings up a fresh one using
    /// the currently selected devices and channel maps. Non-blocking:
    /// permission is requested asynchronously, and on denial `lastError`
    /// is set and nothing else happens.
    func start() {
        stopInternal()
        retryCount = 0
        guard let inDev = selectedInput, let outDev = selectedOutput else {
            lastError = "Select both an input and an output device."
            return
        }
        Task { @MainActor in
            guard await self.requestMicPermission() else {
                self.lastError = "Microphone permission denied. "
                                 + "See System Settings → Privacy → Microphone."
                return
            }
            self.startEngines(input: inDev, output: outDev)
        }
    }

    /// Stops the graph and disarms all observers / pending tasks.
    func stop() {
        removeConfigObservers()
        reconfigureTask?.cancel()
        reconfigureTask = nil
        retryCount = 0
        cooldownUntil = nil
        stopInternal()
    }

    // MARK: - Internal lifecycle

    /// Tears down both engines without touching the observer state.
    /// Idempotent — safe to call when stopped.
    private func stopInternal() {
        // Input side
        if inputEngine.isRunning { inputEngine.stop() }
        if let pipe = inputPipeline {
            inputEngine.disconnectNodeInput(pipe.sinkNode)
            inputEngine.detach(pipe.sinkNode)
        }
        inputPipeline = nil
        inputEngine.reset()

        // Output side
        if outputEngine.isRunning { outputEngine.stop() }
        if let pipe = outputPipeline {
            outputEngine.disconnectNodeOutput(pipe.sourceNode)
            outputEngine.detach(pipe.sourceNode)
        }
        outputPipeline = nil
        outputEngine.disconnectNodeInput(outputMixer)
        outputEngine.disconnectNodeOutput(outputMixer)
        outputEngine.reset()

        ringBuffer.clear()
        isRunning = false
    }

    /// Orchestrates a full graph build-up. On any failure, sets
    /// `lastError` and rolls everything back via `stopInternal()`.
    private func startEngines(input inDev: AudioDevice, output outDev: AudioDevice) {
        do {
            // 1. Bind each engine's AUHAL to the requested device and
            //    confirm the binding actually stuck.
            try bindDevices(input: inDev, output: outDev)

            // 2. Discover the native formats the bound devices expose.
            let (inputFormat, outputFormat) = try queryDeviceFormats()

            // 3. Validate/clamp the channel selections and figure out
            //    how wide the internal bus is going to be.
            let (inCh, outCh, busChannels) = try resolveChannelSelections(
                inputFormat: inputFormat,
                outputFormat: outputFormat)

            // 4. Build the internal format (deinterleaved float at the
            //    OUTPUT sample rate, bus width).
            let internalFormat = try makeInternalFormat(
                sampleRate: outputFormat.sampleRate,
                busChannels: busChannels)

            ringBuffer.reconfigure(channels: busChannels)

            // 5. Wire up the two engines.
            let inputPipe = try buildInputGraph(
                inputFormat: inputFormat,
                internalFormat: internalFormat,
                channels: inCh,
                busChannels: busChannels)

            let outputPipe = try buildOutputGraph(
                outputFormat: outputFormat,
                channels: outCh,
                busChannels: busChannels)

            self.inputPipeline = inputPipe
            self.outputPipeline = outputPipe

            // 6. Start. Output first so the source node has somewhere
            //    to deliver samples by the time the input side begins
            //    producing them.
            outputEngine.prepare()
            try outputEngine.start()
            inputEngine.prepare()
            try inputEngine.start()

            isRunning = true
            lastError = nil
            retryCount = 0

            // 7. Arm the startup cooldown, then attach observers. The
            //    very act of setting the current device earlier in this
            //    method fires a config-change notification; the
            //    cooldown swallows it and prevents a rebuild loop.
            cooldownUntil = Date().addingTimeInterval(Self.startupCooldown)
            installConfigObservers()

            logger.info("""
                Running. in=\(inDev.name, privacy: .public) \
                (\(Int(inputFormat.sampleRate))Hz × \(inputFormat.channelCount)ch) \
                → out=\(outDev.name, privacy: .public) \
                (\(Int(outputFormat.sampleRate))Hz × \(outputFormat.channelCount)ch), \
                bus=\(busChannels)ch
                """)
        } catch {
            lastError = "Engine start failed: \(error.localizedDescription)"
            isRunning = false
            stopInternal()
        }
    }

    // MARK: - Graph building helpers

    /// Binds the input and output AUHAL units to the given devices and
    /// reads the binding back to confirm. If the read-back disagrees
    /// (e.g. the device rejected the change), the method throws — this
    /// is what stops us from silently falling back to the wrong device.
    private func bindDevices(input inDev: AudioDevice,
                             output outDev: AudioDevice) throws {
        try setCurrentDevice(on: outputEngine.outputNode,
                             deviceID: outDev.id, label: "output")
        try setCurrentDevice(on: inputEngine.inputNode,
                             deviceID: inDev.id,  label: "input")

        let boundOut = currentDevice(of: outputEngine.outputNode)
        let boundIn  = currentDevice(of: inputEngine.inputNode)
        logger.info("Bound in=\(boundIn) (wanted \(inDev.id)), out=\(boundOut) (wanted \(outDev.id))")
        if boundOut != outDev.id {
            throw err("Output binding rejected; AUHAL reports \(boundOut), wanted \(outDev.id).")
        }
        if boundIn != inDev.id {
            throw err("Input binding rejected; AUHAL reports \(boundIn), wanted \(inDev.id).")
        }
    }

    /// Fetches the post-binding native formats for both engines and
    /// sanity-checks them.
    private func queryDeviceFormats() throws
        -> (input: AVAudioFormat, output: AVAudioFormat)
    {
        let inFmt  = inputEngine.inputNode.outputFormat(forBus: 0)
        let outFmt = outputEngine.outputNode.inputFormat(forBus: 0)
        guard inFmt.channelCount  > 0, inFmt.sampleRate  > 0,
              outFmt.channelCount > 0, outFmt.sampleRate > 0 else {
            throw err("Device returned an invalid format. in=\(inFmt) out=\(outFmt)")
        }
        return (inFmt, outFmt)
    }

    /// Clamps user-selected channel indices to what each device actually
    /// exposes, refuses to run with a degenerate selection, and returns
    /// the internal bus width (= min of the two selection counts).
    private func resolveChannelSelections(inputFormat: AVAudioFormat,
                                          outputFormat: AVAudioFormat) throws
        -> (input: [Int], output: [Int], busWidth: Int)
    {
        let inCh = inputChannelSelection.filter {
            $0 >= 0 && $0 < Int(inputFormat.channelCount)
        }
        let outCh = outputChannelSelection.filter {
            $0 >= 0 && $0 < Int(outputFormat.channelCount)
        }
        guard !inCh.isEmpty, !outCh.isEmpty else {
            throw err("No valid channels selected. "
                      + "in avail=\(inputFormat.channelCount), "
                      + "out avail=\(outputFormat.channelCount)")
        }
        return (inCh, outCh, min(inCh.count, outCh.count))
    }

    /// Builds the deinterleaved Float32 format used between the sink
    /// and the source nodes (= the ring buffer's carrier format).
    private func makeInternalFormat(sampleRate: Double,
                                    busChannels: Int) throws -> AVAudioFormat {
        guard let fmt = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: sampleRate,
            channels: AVAudioChannelCount(busChannels),
            interleaved: false)
        else {
            throw err("Could not create internal format.")
        }
        return fmt
    }

    /// Constructs the input-side pipeline (sink node + converter +
    /// reusable scratch buffers) and attaches it to `inputEngine`.
    private func buildInputGraph(inputFormat: AVAudioFormat,
                                 internalFormat: AVAudioFormat,
                                 channels: [Int],
                                 busChannels: Int) throws -> InputPipeline
    {
        let pipeline = try InputPipeline(
            inputChannels: channels,
            busChannels: busChannels,
            sourceFormat: inputFormat,
            internalFormat: internalFormat,
            ring: ringBuffer)

        inputEngine.attach(pipeline.sinkNode)
        inputEngine.connect(inputEngine.inputNode,
                            to: pipeline.sinkNode,
                            format: inputFormat)
        return pipeline
    }

    /// Constructs the output-side pipeline (source node) and attaches
    /// it to `outputEngine`.
    private func buildOutputGraph(outputFormat: AVAudioFormat,
                                  channels: [Int],
                                  busChannels: Int) throws -> OutputPipeline
    {
        let pipeline = OutputPipeline(
            outputChannels: channels,
            numOutputChannels: Int(outputFormat.channelCount),
            busChannels: busChannels,
            ring: ringBuffer,
            outputFormat: outputFormat)

        outputEngine.attach(pipeline.sourceNode)
        outputEngine.connect(pipeline.sourceNode,
                             to: outputMixer,
                             format: outputFormat)
        outputEngine.connect(outputMixer,
                             to: outputEngine.outputNode,
                             format: outputFormat)
        outputMixer.outputVolume = volume
        return pipeline
    }

    // MARK: - HAL property helpers

    /// Binds an AVAudioEngine I/O node's underlying AUHAL to the given
    /// CoreAudio device via `kAudioOutputUnitProperty_CurrentDevice`.
    private func setCurrentDevice(on node: AVAudioIONode,
                                  deviceID: AudioDeviceID,
                                  label: String) throws {
        guard let unit = node.audioUnit else {
            throw err("No AudioUnit on \(label) node.")
        }
        var id = deviceID
        let status = AudioUnitSetProperty(
            unit,
            kAudioOutputUnitProperty_CurrentDevice,
            kAudioUnitScope_Global,
            0,
            &id,
            UInt32(MemoryLayout<AudioDeviceID>.size))
        if status != noErr {
            throw err("Couldn't set \(label) device (OSStatus \(status)).")
        }
    }

    /// Reads back the AUHAL's current device. Used to verify that a
    /// `setCurrentDevice` call actually took effect.
    private func currentDevice(of node: AVAudioIONode) -> AudioDeviceID {
        guard let unit = node.audioUnit else { return 0 }
        var id: AudioDeviceID = 0
        var size = UInt32(MemoryLayout<AudioDeviceID>.size)
        let status = AudioUnitGetProperty(
            unit,
            kAudioOutputUnitProperty_CurrentDevice,
            kAudioUnitScope_Global,
            0,
            &id,
            &size)
        return status == noErr ? id : 0
    }

    // MARK: - Configuration-change handling

    /// Subscribes to `AVAudioEngineConfigurationChange` on both engines.
    /// Notifications are coalesced via `scheduleReconfigure()`, and
    /// anything that arrives during the startup cooldown is dropped.
    private func installConfigObservers() {
        removeConfigObservers()

        let nc = NotificationCenter.default

        // The observer block fires on an arbitrary thread (we pass
        // `queue: nil`); hop to the main actor before touching state.
        // The Task takes its own weak capture so the outer closure's
        // captured `self` isn't smuggled into a Sendable context.
        inputConfigObserver = nc.addObserver(
            forName: .AVAudioEngineConfigurationChange,
            object: inputEngine,
            queue: nil
        ) { _ in
            Task { @MainActor [weak self] in self?.scheduleReconfigure() }
        }

        outputConfigObserver = nc.addObserver(
            forName: .AVAudioEngineConfigurationChange,
            object: outputEngine,
            queue: nil
        ) { _ in
            Task { @MainActor [weak self] in self?.scheduleReconfigure() }
        }
    }

    private func removeConfigObservers() {
        let nc = NotificationCenter.default
        if let o = inputConfigObserver  { nc.removeObserver(o); inputConfigObserver  = nil }
        if let o = outputConfigObserver { nc.removeObserver(o); outputConfigObserver = nil }
    }

    /// Debounces bursty config-change notifications into a single
    /// reconfigure. Notifications that arrive during the startup
    /// cooldown are ignored outright (see `cooldownUntil`).
    private func scheduleReconfigure() {
        if let until = cooldownUntil, Date() < until {
            logger.debug("Ignoring config change during startup cooldown.")
            return
        }
        guard !isReconfiguring else { return }

        reconfigureTask?.cancel()
        reconfigureTask = Task { @MainActor [weak self] in
            try? await Task.sleep(nanoseconds: 500_000_000) // 500 ms debounce
            guard let self, !Task.isCancelled else { return }
            self.performReconfigure()
        }
    }

    /// Actually rebuilds the graph. Protected by `isReconfiguring` so
    /// nested notifications can't reenter, and capped by `maxRetries`
    /// so a genuinely broken device can't spin the loop forever.
    private func performReconfigure() {
        guard !isReconfiguring else { return }
        isReconfiguring = true
        defer { isReconfiguring = false }

        retryCount += 1
        let attempt = retryCount
        if attempt > Self.maxRetries {
            logger.error("Giving up after \(Self.maxRetries) reconfigure attempts.")
            lastError = "Audio device reconfiguration failed repeatedly. "
                      + "Please re-select your devices."
            stop()
            return
        }

        logger.info("Audio configuration changed; reconfiguring (attempt \(attempt))…")

        let wasRunning = isRunning
        guard let inDev = selectedInput, let outDev = selectedOutput else {
            stopInternal()
            return
        }
        stopInternal()
        if wasRunning {
            startEngines(input: inDev, output: outDev)
        }
    }

    // MARK: - Error helper

    private func err(_ message: String) -> NSError {
        NSError(domain: "AudioEngineManager", code: -1,
                userInfo: [NSLocalizedDescriptionKey: message])
    }
}

// MARK: - InputPipeline

/// Owns all state consumed by the input-side sink callback.
///
/// The sink callback runs on a CoreAudio-owned realtime thread, so this
/// class must avoid any allocation or locking inside `process(...)`.
/// Everything that could allocate — the two `AVAudioPCMBuffer` scratch
/// buffers, the channel map, the converter — is built once in `init`
/// and reused for the lifetime of the pipeline.
///
/// Marked `@unchecked Sendable` because `AVAudioPCMBuffer` and
/// `AVAudioConverter` aren't `Sendable`, but the audio thread is the
/// sole accessor while the graph is running; the main thread only
/// touches this object during setup and teardown while the engine is
/// stopped.
private final class InputPipeline: @unchecked Sendable {

    /// Largest tap frame count we'll pre-allocate scratch for. Any
    /// callback that exceeds this is processed without reuse (allocates
    /// a fresh buffer just for that call).
    private static let maxFramesPerCallback = 8192

    /// For each output bus channel k, the device input channel to read,
    /// or -1 to silence. Stored as C memory so the audio thread can
    /// iterate without going through Swift's array bounds checks.
    private let inputMap: UnsafeMutablePointer<Int32>
    private let inputMapCount: Int

    /// The user-chosen bus width (= min of both selection sizes).
    private let busChannels: Int

    /// Native format the input device is delivering to us.
    private let sourceFormat: AVAudioFormat

    /// Format written into the ring buffer (= output engine's SR,
    /// `busChannels`, deinterleaved Float32).
    private let internalFormat: AVAudioFormat

    /// Sample-rate converter. Nil when input and output run at the same
    /// rate, in which case we push `picked` directly into the ring.
    private let resampler: AVAudioConverter?

    /// Reused: selected channels copied out of the device ABL, at the
    /// INPUT device's sample rate.
    private let picked: AVAudioPCMBuffer

    /// Reused: output of the resampler, at the OUTPUT device's sample
    /// rate. Nil when `resampler` is nil.
    private let resampled: AVAudioPCMBuffer?

    /// Destination for processed samples.
    private let ring: FloatRingBuffer

    /// The AVAudioSinkNode whose callback invokes `process(...)`.
    /// Created lazily by `makeSinkNode()` so the closure can legally
    /// hold a weak reference back to `self`.
    private(set) var sinkNode: AVAudioSinkNode!

    init(inputChannels: [Int],
         busChannels: Int,
         sourceFormat: AVAudioFormat,
         internalFormat: AVAudioFormat,
         ring: FloatRingBuffer) throws
    {
        self.busChannels = busChannels
        self.sourceFormat = sourceFormat
        self.internalFormat = internalFormat
        self.ring = ring

        // Build the fixed-size input map (busChannels entries).
        self.inputMapCount = busChannels
        self.inputMap = UnsafeMutablePointer<Int32>.allocate(capacity: busChannels)
        for k in 0..<busChannels {
            self.inputMap[k] = k < inputChannels.count
                ? Int32(inputChannels[k])
                : -1
        }

        // Scratch "picked" buffer: the same channels the user chose,
        // still at the input device's sample rate, deinterleaved float.
        guard let preFmt = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: sourceFormat.sampleRate,
            channels: AVAudioChannelCount(busChannels),
            interleaved: false),
              let picked = AVAudioPCMBuffer(
                pcmFormat: preFmt,
                frameCapacity: AVAudioFrameCount(Self.maxFramesPerCallback))
        else {
            self.inputMap.deallocate()
            throw NSError(
                domain: "InputPipeline", code: -1,
                userInfo: [NSLocalizedDescriptionKey:
                            "Failed to allocate picked scratch buffer."])
        }
        self.picked = picked

        // Resampler + reused output buffer, only if rates differ.
        if sourceFormat.sampleRate != internalFormat.sampleRate {
            guard let conv = AVAudioConverter(from: preFmt, to: internalFormat) else {
                self.inputMap.deallocate()
                throw NSError(
                    domain: "InputPipeline", code: -2,
                    userInfo: [NSLocalizedDescriptionKey:
                                "Failed to create resampler."])
            }
            let ratio = internalFormat.sampleRate / sourceFormat.sampleRate
            let outCap = AVAudioFrameCount(
                Double(Self.maxFramesPerCallback) * ratio) + 1024
            guard let out = AVAudioPCMBuffer(
                pcmFormat: internalFormat,
                frameCapacity: outCap)
            else {
                self.inputMap.deallocate()
                throw NSError(
                    domain: "InputPipeline", code: -3,
                    userInfo: [NSLocalizedDescriptionKey:
                                "Failed to allocate resample scratch."])
            }
            self.resampler = conv
            self.resampled = out
        } else {
            self.resampler = nil
            self.resampled = nil
        }

        // Build the sink node last, via a helper method so that
        // self is fully initialised and we can take a weak reference.
        self.sinkNode = nil
        self.sinkNode = makeSinkNode()
    }

    deinit {
        inputMap.deallocate()
    }

    /// Creates the sink node that feeds `process(...)`. Called from
    /// `init`; isolated in its own method so `[weak self]` captures
    /// against a fully-initialised `self`.
    private func makeSinkNode() -> AVAudioSinkNode {
        AVAudioSinkNode { [weak self] _, frameCount, ablPtr -> OSStatus in
            // If the pipeline has already been torn down the engine
            // has also detached the node, so this path should be
            // unreachable; guard anyway for safety.
            self?.process(ablPtr: ablPtr, frameCount: Int(frameCount))
            return noErr
        }
    }

    // MARK: Audio-thread path

    /// Called on CoreAudio's realtime thread for every input tap block.
    /// Must be allocation- and lock-minimal.
    ///
    /// Steps:
    ///   1. Extract user-selected channels from the device ABL into
    ///      the pre-allocated `picked` buffer. Any unselected bus
    ///      channel is zeroed.
    ///   2. If a resampler is installed, run it and push the result
    ///      into the ring. Otherwise push `picked` directly.
    private func process(ablPtr: UnsafePointer<AudioBufferList>,
                         frameCount: Int) {
        guard frameCount > 0, frameCount <= picked.frameCapacity else { return }

        let abl = UnsafeMutableAudioBufferListPointer(
            UnsafeMutablePointer(mutating: ablPtr))
        guard let dstChannels = picked.floatChannelData else { return }

        extractChannels(from: abl,
                        into: dstChannels,
                        frameCount: frameCount)
        picked.frameLength = AVAudioFrameCount(frameCount)

        if let conv = resampler, let out = resampled {
            runResampler(conv: conv, output: out)
        } else {
            ring.write(from: picked)
        }
    }

    /// Pulls `busChannels` channels out of the raw device buffer list
    /// according to `inputMap`, writing into the pre-allocated `picked`
    /// scratch. Unselected bus slots are zeroed.
    ///
    /// Handles both deinterleaved (the normal AVAudioEngine path) and
    /// interleaved device buffer layouts.
    private func extractChannels(
        from abl: UnsafeMutableAudioBufferListPointer,
        into dst: UnsafePointer<UnsafeMutablePointer<Float>>,
        frameCount: Int
    ) {
        let bytes = frameCount * MemoryLayout<Float>.size

        if sourceFormat.isInterleaved {
            let deviceCh = Int(abl[0].mNumberChannels)
            guard let raw = abl[0].mData?.assumingMemoryBound(to: Float.self) else {
                for k in 0..<busChannels { memset(dst[k], 0, bytes) }
                return
            }
            for k in 0..<busChannels {
                let devIdx = Int(inputMap[k])
                if devIdx >= 0 && devIdx < deviceCh {
                    // Strided copy: pick one column out of the
                    // interleaved matrix.
                    for f in 0..<frameCount {
                        dst[k][f] = raw[f * deviceCh + devIdx]
                    }
                } else {
                    memset(dst[k], 0, bytes)
                }
            }
        } else {
            let deviceCh = abl.count
            for k in 0..<busChannels {
                let devIdx = Int(inputMap[k])
                if devIdx >= 0 && devIdx < deviceCh,
                   let src = abl[devIdx].mData?.assumingMemoryBound(to: Float.self) {
                    memcpy(dst[k], src, bytes)
                } else {
                    memset(dst[k], 0, bytes)
                }
            }
        }
    }

    /// Drives `AVAudioConverter.convert` with the pre-allocated `picked`
    /// buffer as the one-shot input source, and writes the resulting
    /// resampled frames into the ring.
    private func runResampler(conv: AVAudioConverter,
                              output out: AVAudioPCMBuffer) {
        var provided = false
        let src = picked
        var convErr: NSError?
        let status = conv.convert(to: out, error: &convErr) { _, inStatus in
            if provided {
                inStatus.pointee = .noDataNow
                return nil
            }
            provided = true
            inStatus.pointee = .haveData
            return src
        }
        if status == .error {
            if let e = convErr {
                logger.warning("Resample error: \(e.localizedDescription, privacy: .public)")
            }
            return
        }
        ring.write(from: out)
    }
}

// MARK: - OutputPipeline

/// Owns all state consumed by the output-side source-node render block.
///
/// Unlike `InputPipeline` the output side doesn't need scratch buffers:
/// the render block writes directly into the AudioBufferList provided
/// by the engine, picking samples out of the ring buffer.
///
/// `@unchecked Sendable` — see the note on `InputPipeline`.
private final class OutputPipeline: @unchecked Sendable {

    /// For each bus channel k, the device output channel to write to,
    /// or -1 to silence. Stored in C memory for the same reason as
    /// `InputPipeline.inputMap`.
    private let outputMap: UnsafeMutablePointer<Int32>

    private let numOutputChannels: Int
    private let busChannels: Int
    private let ring: FloatRingBuffer

    /// The AVAudioSourceNode whose render block invokes `render(...)`.
    /// Created lazily by `makeSourceNode()` so the closure can legally
    /// hold a weak reference back to `self`.
    private(set) var sourceNode: AVAudioSourceNode!

    init(outputChannels: [Int],
         numOutputChannels: Int,
         busChannels: Int,
         ring: FloatRingBuffer,
         outputFormat: AVAudioFormat)
    {
        self.numOutputChannels = numOutputChannels
        self.busChannels = busChannels
        self.ring = ring

        self.outputMap = UnsafeMutablePointer<Int32>.allocate(capacity: busChannels)
        for k in 0..<busChannels {
            self.outputMap[k] = k < outputChannels.count
                ? Int32(outputChannels[k])
                : -1
        }

        self.sourceNode = nil
        self.sourceNode = makeSourceNode(outputFormat: outputFormat)
    }

    deinit {
        outputMap.deallocate()
    }

    /// Creates the source node that delegates to `render(...)`. Called
    /// from `init`; isolated in its own method so `[weak self]`
    /// captures against a fully-initialised `self`.
    private func makeSourceNode(outputFormat: AVAudioFormat) -> AVAudioSourceNode {
        AVAudioSourceNode(format: outputFormat) {
            [weak self] _, _, frameCount, ablPtr -> OSStatus in
            guard let self else { return noErr }
            return self.render(frameCount: Int(frameCount), ablPtr: ablPtr)
        }
    }

    // MARK: Audio-thread path

    /// Source-node render block. Zero-fills every output channel, then
    /// fans each bus channel of the ring out to the selected device
    /// output channel.
    private func render(frameCount: Int,
                        ablPtr: UnsafeMutablePointer<AudioBufferList>) -> OSStatus
    {
        let abl = UnsafeMutableAudioBufferListPointer(ablPtr)

        // Silence every device channel up front. The ring buffer
        // will then overwrite only the channels we actually route to.
        for i in 0..<abl.count {
            if let data = abl[i].mData {
                memset(data, 0, Int(abl[i].mDataByteSize))
            }
        }

        ring.read(frames: frameCount,
                  busChannels: busChannels,
                  outputMap: outputMap,
                  numOutputChannels: numOutputChannels,
                  abl: abl)
        return noErr
    }
}

// MARK: - FloatRingBuffer

/// Deinterleaved Float32 SPSC ring buffer.
///
/// Layout is a single contiguous block: for `channels` channels and a
/// capacity of `capacityFrames`, storage is laid out channel-major, so
/// channel c's samples are at `storage[c * capacityFrames ..< (c+1) * capacityFrames]`.
///
/// A single `OSAllocatedUnfairLock` serializes the read and write
/// pointers. It's marginally slower than a true lock-free SPSC queue
/// but vastly simpler and contention between the input audio thread
/// and the output audio thread is essentially zero under normal load.
///
/// Writer overflow policy: drop the oldest frames to make room. The
/// buffer never blocks the audio thread.
///
/// Reader underflow policy: copy only what's available; the caller is
/// expected to have pre-zeroed its destination.
final class FloatRingBuffer: @unchecked Sendable {

    private var storage: UnsafeMutablePointer<Float>
    private let capacityFrames: Int
    private(set) var channels: Int
    private var writeIdx = 0
    private var readIdx  = 0
    private var filled   = 0
    private let lock = OSAllocatedUnfairLock()

    init(frameCapacity: Int, initialChannels: Int) {
        self.capacityFrames = frameCapacity
        self.channels = max(initialChannels, 1)
        let total = capacityFrames * self.channels
        self.storage = UnsafeMutablePointer<Float>.allocate(capacity: total)
        self.storage.initialize(repeating: 0, count: total)
    }

    deinit {
        storage.deinitialize(count: capacityFrames * channels)
        storage.deallocate()
    }

    // MARK: Setup

    /// Switches to a new channel count, reallocating the backing store
    /// if necessary. Safe to call while the engine is stopped; unsafe
    /// to call while audio is flowing.
    func reconfigure(channels newChannels: Int) {
        lock.lock(); defer { lock.unlock() }
        if newChannels != channels {
            storage.deinitialize(count: capacityFrames * channels)
            storage.deallocate()
            channels = max(newChannels, 1)
            let total = capacityFrames * channels
            storage = UnsafeMutablePointer<Float>.allocate(capacity: total)
            storage.initialize(repeating: 0, count: total)
        }
        writeIdx = 0
        readIdx = 0
        filled = 0
    }

    /// Drops all buffered samples. Read/write indices are not
    /// deallocated.
    func clear() {
        lock.lock(); defer { lock.unlock() }
        writeIdx = 0
        readIdx = 0
        filled = 0
    }

    // MARK: Producer

    /// Copy every channel of a deinterleaved Float32 PCM buffer into
    /// the ring. If the incoming chunk is larger than the ring's total
    /// capacity, only the most recent samples are kept. If the ring
    /// is already full, the oldest samples are dropped to make room.
    func write(from buffer: AVAudioPCMBuffer) {
        guard let chs = buffer.floatChannelData else { return }
        var frames = Int(buffer.frameLength)
        if frames <= 0 { return }

        lock.lock(); defer { lock.unlock() }

        // Clamp oversized chunks to just the tail that would fit.
        var srcOffset = 0
        if frames > capacityFrames {
            srcOffset = frames - capacityFrames
            frames = capacityFrames
        }

        // Drop old samples to make room.
        let overflow = (filled + frames) - capacityFrames
        if overflow > 0 {
            let drop = min(overflow, filled)
            readIdx = (readIdx + drop) % capacityFrames
            filled -= drop
        }

        let srcCh = Int(buffer.format.channelCount)
        for c in 0..<channels {
            if c < srcCh {
                let src = chs[c] + srcOffset
                writeChannel(c: c, src: src, frames: frames)
            } else {
                // Producer has fewer channels than the ring — zero-fill
                // the unused bus slots.
                zeroChannel(c: c, startAt: writeIdx, frames: frames)
            }
        }
        writeIdx = (writeIdx + frames) % capacityFrames
        filled = min(capacityFrames, filled + frames)
    }

    /// Copy `frames` samples of channel `c` from `src` into the ring,
    /// handling the wrap-around in at most two `memcpy` calls.
    private func writeChannel(c: Int, src: UnsafePointer<Float>, frames: Int) {
        let base = c * capacityFrames
        let first = min(frames, capacityFrames - writeIdx)
        memcpy(storage + base + writeIdx, src, first * MemoryLayout<Float>.size)
        let rest = frames - first
        if rest > 0 {
            memcpy(storage + base, src + first, rest * MemoryLayout<Float>.size)
        }
    }

    /// Zero `frames` samples of channel `c` starting at `offset`,
    /// handling the wrap-around.
    private func zeroChannel(c: Int, startAt offset: Int, frames: Int) {
        let base = c * capacityFrames
        let first = min(frames, capacityFrames - offset)
        memset(storage + base + offset, 0, first * MemoryLayout<Float>.size)
        let rest = frames - first
        if rest > 0 {
            memset(storage + base, 0, rest * MemoryLayout<Float>.size)
        }
    }

    // MARK: Consumer

    /// Fan out up to `frames` frames from the ring into the device
    /// output channels named by `outputMap`. The caller is expected to
    /// have zeroed the destination buffer list; only channels named by
    /// the map are overwritten.
    ///
    /// - Parameters:
    ///   - frames: Number of output frames the render block requested.
    ///   - busChannels: Number of ring-buffer channels to read (= bus width).
    ///   - outputMap: C array of length `busChannels`. For each bus
    ///                channel k, `outputMap[k]` is the 0-based device
    ///                output channel to write to, or -1 to silence.
    ///   - numOutputChannels: Guardrail for `outputMap` entries.
    ///   - abl: The AudioBufferList the render block was handed.
    func read(frames: Int,
              busChannels: Int,
              outputMap: UnsafePointer<Int32>,
              numOutputChannels: Int,
              abl: UnsafeMutableAudioBufferListPointer) {
        lock.lock(); defer { lock.unlock() }

        let available = min(frames, filled)
        if available == 0 { return }

        let ringCh = min(busChannels, channels)
        for k in 0..<ringCh {
            let devCh = Int(outputMap[k])
            guard devCh >= 0, devCh < numOutputChannels, devCh < abl.count,
                  let rawDst = abl[devCh].mData else { continue }
            let dst = rawDst.assumingMemoryBound(to: Float.self)

            let base = k * capacityFrames
            let first = min(available, capacityFrames - readIdx)
            memcpy(dst, storage + base + readIdx, first * MemoryLayout<Float>.size)
            let rest = available - first
            if rest > 0 {
                memcpy(dst + first, storage + base, rest * MemoryLayout<Float>.size)
            }
        }

        readIdx = (readIdx + available) % capacityFrames
        filled -= available
    }
}
