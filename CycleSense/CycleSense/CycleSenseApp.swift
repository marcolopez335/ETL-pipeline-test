import SwiftUI

@main
struct CycleSenseApp: App {
    @StateObject private var store = CycleStore()

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(store)
        }
    }
}
