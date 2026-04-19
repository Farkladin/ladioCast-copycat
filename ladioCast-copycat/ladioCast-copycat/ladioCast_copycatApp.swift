//
//  ladioCast_copycatApp.swift
//  ladioCast-copycat
//
//  Menu-bar-only audio router / mixer.
//

import SwiftUI

@main
struct ladioCast_copycatApp: App {

    @StateObject private var devices = AudioDeviceManager()
    @StateObject private var engine  = AudioEngineManager()

    var body: some Scene {
        MenuBarExtra("AudioRouter", systemImage: "waveform") {
            MenuBarView(devices: devices, engine: engine)
        }
        .menuBarExtraStyle(.window)
    }
}
