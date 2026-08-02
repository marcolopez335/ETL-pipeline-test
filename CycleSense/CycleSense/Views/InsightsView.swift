import SwiftUI

struct InsightsView: View {
    @EnvironmentObject private var store: CycleStore

    var body: some View {
        NavigationStack {
            Group {
                if store.cycles.isEmpty {
                    ContentUnavailableView(
                        "No cycles yet",
                        systemImage: "calendar.badge.exclamationmark",
                        description: Text("Log your first period from the Today or Calendar tab and your insights will appear here.")
                    )
                } else {
                    insightsList
                }
            }
            .navigationTitle("Insights")
        }
    }

    private var insightsList: some View {
        List {
            Section("Averages") {
                LabeledContent("Average cycle length", value: lengthText(store.prediction?.averageCycleLength))
                LabeledContent("Average period length", value: lengthText(store.prediction?.averagePeriodLength))
                LabeledContent("Cycles tracked", value: "\(store.cycles.count)")
            }

            if let prediction = store.prediction {
                Section("Upcoming") {
                    LabeledContent(
                        "Next period",
                        value: prediction.nextPeriodStart.formatted(date: .abbreviated, time: .omitted)
                    )
                    if let window = prediction.fertileWindow {
                        LabeledContent(
                            "Fertile window",
                            value: "\(window.start.formatted(.dateTime.month(.abbreviated).day())) – \(window.end.formatted(.dateTime.month(.abbreviated).day()))"
                        )
                    }
                    if let ovulation = prediction.ovulationDate {
                        LabeledContent(
                            "Estimated ovulation",
                            value: ovulation.formatted(date: .abbreviated, time: .omitted)
                        )
                    }
                }
            }

            Section("History") {
                ForEach(store.cycles.reversed()) { cycle in
                    HStack {
                        VStack(alignment: .leading, spacing: 2) {
                            Text(cycle.start.formatted(date: .abbreviated, time: .omitted))
                                .font(.body)
                            Text("Period: \(cycle.periodLength) \(cycle.periodLength == 1 ? "day" : "days")")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        Spacer()
                        if let length = cycle.cycleLength {
                            Text("\(length)-day cycle")
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                        } else {
                            Text("Ongoing")
                                .font(.subheadline)
                                .foregroundStyle(.pink)
                        }
                    }
                }
            }

            Section {
            } footer: {
                Text("Averages use your last \(CyclePredictor.historyWindow) cycles. Predictions are estimates for informational purposes only and are not medical advice or contraception.")
            }
        }
    }

    private func lengthText(_ value: Int?) -> String {
        guard let value else { return "—" }
        return "\(value) days"
    }
}

#Preview {
    InsightsView()
        .environmentObject(CycleStore())
}
