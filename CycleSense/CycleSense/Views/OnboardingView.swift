import SwiftUI

struct OnboardingView: View {
    @EnvironmentObject private var store: CycleStore
    @AppStorage("hasCompletedOnboarding") private var hasCompletedOnboarding = false
    @State private var isRequesting = false

    var body: some View {
        VStack(spacing: 16) {
            Spacer()

            Image(systemName: "heart.circle.fill")
                .font(.system(size: 72))
                .foregroundStyle(.pink)
            Text("Welcome to CycleSense")
                .font(.largeTitle.bold())
                .multilineTextAlignment(.center)
            Text("Track your cycle, understand your body.")
                .foregroundStyle(.secondary)

            VStack(alignment: .leading, spacing: 20) {
                OnboardingRow(
                    icon: "drop.fill",
                    tint: .red,
                    title: "Log your period",
                    detail: "Record flow and symptoms in seconds."
                )
                OnboardingRow(
                    icon: "calendar",
                    tint: .pink,
                    title: "See what is ahead",
                    detail: "Estimates for your next period, fertile window, and ovulation."
                )
                OnboardingRow(
                    icon: "heart.text.square.fill",
                    tint: .green,
                    title: "Synced with Apple Health",
                    detail: "Everything you log lives in Health, under your control. Nothing leaves your device."
                )
            }
            .padding(.vertical, 24)

            Spacer()

            if !store.healthAvailable {
                Text("Health data is not available on this device.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }

            Button(action: connect) {
                Text(store.healthAvailable ? "Connect Apple Health" : "Continue")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderedProminent)
            .controlSize(.large)
            .disabled(isRequesting)

            Text("CycleSense is not a medical device. Predictions are estimates — do not rely on them for contraception.")
                .font(.caption2)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
        }
        .padding(24)
    }

    private func connect() {
        isRequesting = true
        Task {
            await store.requestAccessAndRefresh()
            isRequesting = false
            hasCompletedOnboarding = true
        }
    }
}

private struct OnboardingRow: View {
    let icon: String
    let tint: Color
    let title: String
    let detail: String

    var body: some View {
        HStack(alignment: .top, spacing: 16) {
            Image(systemName: icon)
                .font(.title2)
                .foregroundStyle(tint)
                .frame(width: 32)
            VStack(alignment: .leading, spacing: 2) {
                Text(title).font(.headline)
                Text(detail)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }
        }
    }
}

#Preview {
    OnboardingView()
        .environmentObject(CycleStore())
}
