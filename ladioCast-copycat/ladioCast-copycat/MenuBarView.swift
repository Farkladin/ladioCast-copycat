//
//  MenuBarView.swift
//  ladioCast-copycat
//
//  The SwiftUI content hosted inside MenuBarExtra.
//

import SwiftUI

struct MenuBarView: View {

    @ObservedObject var devices: AudioDeviceManager
    @ObservedObject var engine:  AudioEngineManager

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {

            // HEADER
            HStack(spacing: 8) {
                Image(systemName: "waveform.circle.fill")
                    .font(.title2)
                    .foregroundColor(engine.isRunning ? .green : .gray)

                Text("AudioRouter")
                    .font(.headline)

                Spacer()

                Circle()
                    .fill(engine.isRunning ? Color.green : Color.gray)
                    .frame(width: 8, height: 8)
            }

            Divider()

            // INPUT DEVICE
            Text("Input Device")
                .font(.caption)
                .foregroundColor(.secondary)

            Picker("Input", selection: $engine.selectedInput) {
                Text("— none —").tag(Optional<AudioDevice>.none)
                ForEach(devices.inputDevices) { dev in
                    Text("\(dev.name) (\(dev.inputChannels)ch)")
                        .tag(Optional<AudioDevice>.some(dev))
                }
            }
            .labelsHidden()

            if let dev = engine.selectedInput {
                channelGrid(
                    count: dev.inputChannels,
                    selection: $engine.inputChannelSelection,
                    color: .cyan
                )
            }

            Divider()

            // OUTPUT DEVICE
            Text("Output Device")
                .font(.caption)
                .foregroundColor(.secondary)

            Picker("Output", selection: $engine.selectedOutput) {
                Text("— none —").tag(Optional<AudioDevice>.none)
                ForEach(devices.outputDevices) { dev in
                    Text("\(dev.name) (\(dev.outputChannels)ch)")
                        .tag(Optional<AudioDevice>.some(dev))
                }
            }
            .labelsHidden()

            if let dev = engine.selectedOutput {
                channelGrid(
                    count: dev.outputChannels,
                    selection: $engine.outputChannelSelection,
                    color: .orange
                )
            }

            Divider()

            // VOLUME
            Text("Volume")
                .font(.caption)
                .foregroundColor(.secondary)

            HStack(spacing: 6) {
                Image(systemName: "speaker.fill")
                    .font(.caption2)
                Slider(value: $engine.volume, in: 0...1)
                Image(systemName: "speaker.wave.3.fill")
                    .font(.caption2)
                Text("\(Int(engine.volume * 100))%")
                    .font(.caption)
                    .monospacedDigit()
                    .frame(width: 36, alignment: .trailing)
            }

            // ERROR
            if let err = engine.lastError {
                Text(err)
                    .font(.caption2)
                    .foregroundColor(.red)
                    .padding(4)
            }

            Divider()

            // CONTROLS
            HStack {
                Button(engine.isRunning ? "■ Stop" : "▶ Start") {
                    if engine.isRunning { engine.stop() } else { engine.start() }
                }
                .keyboardShortcut(.defaultAction)

                Button("↻ Refresh") {
                    devices.refresh()
                }

                Spacer()

                Button("Quit") {
                    NSApp.terminate(nil)
                }
                .keyboardShortcut("q")
            }
        }
        .padding(14)
        .frame(width: 320)
    }

    // MARK: - Channel Grid

    @ViewBuilder
    private func channelGrid(count: Int, selection: Binding<[Int]>, color: Color) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            let gridColumns = Array(repeating: GridItem(.flexible(), spacing: 4), count: 4)

            LazyVGrid(columns: gridColumns, alignment: .leading, spacing: 4) {
                ForEach(0..<count, id: \.self) { idx in
                    let isOn = selection.wrappedValue.contains(idx)
                    let orderIdx = selection.wrappedValue.firstIndex(of: idx)

                    Button {
                        var sel = selection.wrappedValue
                        if let pos = sel.firstIndex(of: idx) {
                            sel.remove(at: pos)
                        } else {
                            sel.append(idx)
                        }
                        selection.wrappedValue = sel
                    } label: {
                        HStack(spacing: 2) {
                            if let orderIdx {
                                let label: String = orderIdx == 0 ? "L" : (orderIdx == 1 ? "R" : "\(orderIdx+1)")
                                Text(label)
                                    .font(.system(size: 9, weight: .bold, design: .monospaced))
                                    .foregroundColor(.white)
                                    .frame(width: 13, height: 13)
                                    .background(Circle().fill(color))
                            }
                            Text("Ch\(idx + 1)")
                                .font(.caption2)
                        }
                        .padding(.horizontal, 5)
                        .padding(.vertical, 3)
                        .frame(maxWidth: .infinity)
                        .background(
                            RoundedRectangle(cornerRadius: 4)
                                .fill(isOn ? color.opacity(0.15) : Color.clear)
                        )
                        .overlay(
                            RoundedRectangle(cornerRadius: 4)
                                .stroke(isOn ? color.opacity(0.6) : Color.gray.opacity(0.3), lineWidth: 1)
                        )
                    }
                    .buttonStyle(.plain)
                }
            }

            // Mapping summary
            if !selection.wrappedValue.isEmpty {
                let summary = selection.wrappedValue.enumerated().map { i, ch in
                    let bus = i == 0 ? "L" : (i == 1 ? "R" : "\(i+1)")
                    return "Ch\(ch+1)→\(bus)"
                }.joined(separator: " ")
                Text(summary)
                    .font(.caption2)
                    .monospacedDigit()
                    .foregroundColor(.secondary)
            }
        }
    }
}
