import SwiftUI

struct ContentView: View {
    @EnvironmentObject private var store: CycleStore
    @AppStorage("hasCompletedOnboarding") private var hasCompletedOnboarding = false

    var body: some View {
        if hasCompletedOnboarding {
            TabView {
                TodayView()
                    .tabItem { Label("Today", systemImage: "sun.max.fill") }
                CycleCalendarView()
                    .tabItem { Label("Calendar", systemImage: "calendar") }
                InsightsView()
                    .tabItem { Label("Insights", systemImage: "chart.line.uptrend.xyaxis") }
                SettingsView()
                    .tabItem { Label("Settings", systemImage: "gearshape.fill") }
            }
            .task { await store.refresh() }
            .alert(
                "Something went wrong",
                isPresented: Binding(
                    get: { store.lastError != nil },
                    set: { if !$0 { store.lastError = nil } }
                )
            ) {
                Button("OK", role: .cancel) {}
            } message: {
                Text(store.lastError ?? "")
            }
        } else {
            OnboardingView()
        }
    }
}

#Preview {
    ContentView()
        .environmentObject(CycleStore())
}
