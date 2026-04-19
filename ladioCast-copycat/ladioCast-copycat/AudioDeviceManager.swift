//
//  AudioDeviceManager.swift
//  ladioCast-copycat
//
//  Discovers CoreAudio input/output devices and their channel counts.
//

import Foundation
import Combine
import CoreAudio
import AudioToolbox

struct AudioDevice: Identifiable, Hashable, Sendable {
    let id: AudioDeviceID       // CoreAudio device ID
    let uid: String             // Persistent UID
    let name: String
    let inputChannels: Int      // Number of channels on input scope
    let outputChannels: Int     // Number of channels on output scope

    var hasInput: Bool { inputChannels > 0 }
    var hasOutput: Bool { outputChannels > 0 }
}

enum AudioDeviceScope {
    case input, output
    var propertyScope: AudioObjectPropertyScope {
        switch self {
        case .input:  return kAudioDevicePropertyScopeInput
        case .output: return kAudioDevicePropertyScopeOutput
        }
    }
}

@MainActor
final class AudioDeviceManager: ObservableObject {

    @Published private(set) var devices: [AudioDevice] = []

    init() {
        refresh()
        installDeviceListListener()
    }

    nonisolated deinit {
        removeDeviceListListener()
    }

    // MARK: - Device enumeration

    func refresh() {
        devices = Self.fetchAllDevices()
    }

    var inputDevices: [AudioDevice] { devices.filter { $0.hasInput } }
    var outputDevices: [AudioDevice] { devices.filter { $0.hasOutput } }

    private static func fetchAllDevices() -> [AudioDevice] {
        var addr = AudioObjectPropertyAddress(
            mSelector: kAudioHardwarePropertyDevices,
            mScope: kAudioObjectPropertyScopeGlobal,
            mElement: kAudioObjectPropertyElementMain
        )

        var dataSize: UInt32 = 0
        guard AudioObjectGetPropertyDataSize(
            AudioObjectID(kAudioObjectSystemObject), &addr, 0, nil, &dataSize
        ) == noErr else { return [] }

        let count = Int(dataSize) / MemoryLayout<AudioDeviceID>.size
        var ids = [AudioDeviceID](repeating: 0, count: count)

        guard AudioObjectGetPropertyData(
            AudioObjectID(kAudioObjectSystemObject), &addr, 0, nil, &dataSize, &ids
        ) == noErr else { return [] }

        return ids.compactMap { makeDevice(from: $0) }
    }

    private static func makeDevice(from id: AudioDeviceID) -> AudioDevice? {
        guard let name = stringProperty(id: id,
                                        selector: kAudioObjectPropertyName,
                                        scope: kAudioObjectPropertyScopeGlobal) else {
            return nil
        }
        let uid = stringProperty(id: id,
                                 selector: kAudioDevicePropertyDeviceUID,
                                 scope: kAudioObjectPropertyScopeGlobal) ?? ""

        let ins  = channelCount(id: id, scope: kAudioDevicePropertyScopeInput)
        let outs = channelCount(id: id, scope: kAudioDevicePropertyScopeOutput)

        // Skip "devices" that have neither input nor output (aggregate placeholders)
        guard ins > 0 || outs > 0 else { return nil }

        return AudioDevice(id: id, uid: uid, name: name,
                           inputChannels: ins, outputChannels: outs)
    }

    private static func stringProperty(id: AudioDeviceID,
                                       selector: AudioObjectPropertySelector,
                                       scope: AudioObjectPropertyScope) -> String? {
        var addr = AudioObjectPropertyAddress(
            mSelector: selector,
            mScope: scope,
            mElement: kAudioObjectPropertyElementMain
        )
        var cf: CFString = "" as CFString
        var size = UInt32(MemoryLayout<CFString>.size)
        let status = withUnsafeMutablePointer(to: &cf) {
            AudioObjectGetPropertyData(id, &addr, 0, nil, &size, $0)
        }
        guard status == noErr else { return nil }
        return cf as String
    }

    /// Sum the channel counts across all streams in the given scope.
    private static func channelCount(id: AudioDeviceID,
                                     scope: AudioObjectPropertyScope) -> Int {
        var addr = AudioObjectPropertyAddress(
            mSelector: kAudioDevicePropertyStreamConfiguration,
            mScope: scope,
            mElement: kAudioObjectPropertyElementMain
        )
        var size: UInt32 = 0
        guard AudioObjectGetPropertyDataSize(id, &addr, 0, nil, &size) == noErr,
              size > 0 else { return 0 }

        let bufferList = UnsafeMutablePointer<AudioBufferList>.allocate(
            capacity: Int(size))
        defer { bufferList.deallocate() }

        guard AudioObjectGetPropertyData(id, &addr, 0, nil, &size, bufferList) == noErr else {
            return 0
        }
        let abl = UnsafeMutableAudioBufferListPointer(bufferList)
        return abl.reduce(0) { $0 + Int($1.mNumberChannels) }
    }

    // MARK: - Listener for device hot-plug

    private nonisolated(unsafe) var listenerAddr = AudioObjectPropertyAddress(
        mSelector: kAudioHardwarePropertyDevices,
        mScope: kAudioObjectPropertyScopeGlobal,
        mElement: kAudioObjectPropertyElementMain
    )
    private nonisolated(unsafe) var listenerBlock: AudioObjectPropertyListenerBlock?

    private func installDeviceListListener() {
        let block: AudioObjectPropertyListenerBlock = { [weak self] _, _ in
            Task { @MainActor in
                self?.refresh()
            }
        }
        listenerBlock = block
        AudioObjectAddPropertyListenerBlock(
            AudioObjectID(kAudioObjectSystemObject),
            &listenerAddr, DispatchQueue.main, block)
    }

    private nonisolated func removeDeviceListListener() {
        guard let block = listenerBlock else { return }
        var addr = listenerAddr
        AudioObjectRemovePropertyListenerBlock(
            AudioObjectID(kAudioObjectSystemObject),
            &addr, DispatchQueue.main, block)
    }
}
