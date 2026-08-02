import SwiftUI

struct SettingsView: View {
    @EnvironmentObject private var store: CycleStore
    @Environment(\.openURL) private var openURL

    var body: some View {
        NavigationStack {
            List {
                Section {
                    LabeledContent("Health data", value: store.healthAvailable ? "Available" : "Unavailable")
                    Button("Re-request Health permissions") {
                        Task { await store.requestAccessAndRefresh() }
                    }
                    .disabled(!store.healthAvailable)
                    Button("Open Health app") {
                        if let url = URL(string: "x-apple-health://") {
                            openURL(url)
                        }
                    }
                } header: {
                    Text("Apple Health")
                } footer: {
                    Text("iOS only shows the permission sheet once. To change access later, open the Health app and go to Sharing → Apps → CycleSense.")
                }

                Section {
                    Button("Refresh data from Health") {
                        Task { await store.refresh() }
                    }
                    .disabled(store.isRefreshing || !store.healthAvailable)
                } footer: {
                    Text("CycleSense stores nothing outside Apple Health. Deleting the app never deletes your Health data.")
                }

                Section {
                    LabeledContent("Version", value: "1.0")
                } header: {
                    Text("About")
                } footer: {
                    Text("CycleSense is not a medical device. Cycle, fertility, and ovulation predictions are estimates for informational purposes only — do not use them as contraception or medical advice. Talk to a healthcare provider about anything that concerns you.")
                }
            }
            .navigationTitle("Settings")
        }
    }
}

#Preview {
    SettingsView()
        .environmentObject(CycleStore())
}
