import Foundation
import SwiftUI

/// Observable source of truth for the UI. Mirrors HealthKit data into
/// day-keyed dictionaries and derives cycles and predictions from them.
@MainActor
final class CycleStore: ObservableObject {
    /// Logged flow keyed by start-of-day.
    @Published private(set) var flowByDay: [Date: FlowLevel] = [:]
    /// Logged symptoms keyed by start-of-day.
    @Published private(set) var symptomsByDay: [Date: Set<Symptom>] = [:]
    /// Cycles derived from `flowByDay`, oldest first.
    @Published private(set) var cycles: [Cycle] = []
    @Published private(set) var prediction: Prediction?
    @Published private(set) var isRefreshing = false
    @Published var lastError: String?

    let healthAvailable = HealthKitManager.isHealthDataAvailable

    private let calendar = Calendar.current
    private let manager = HealthKitManager.shared

    /// How a calendar day should be marked.
    enum DayMark {
        case period(FlowLevel)
        case predictedPeriod
        case fertile
        case ovulation
    }

    // MARK: - HealthKit lifecycle

    func requestAccessAndRefresh() async {
        guard healthAvailable else { return }
        do {
            try await manager.requestAuthorization()
            await refresh()
        } catch {
            lastError = error.localizedDescription
        }
    }

    /// Reloads the last two years of data from HealthKit.
    func refresh() async {
        guard healthAvailable else { return }
        isRefreshing = true
        defer { isRefreshing = false }
        do {
            async let flow = manager.fetchFlowByDay(monthsBack: 24)
            async let symptoms = manager.fetchSymptomsByDay(monthsBack: 24)
            flowByDay = try await flow
            symptomsByDay = try await symptoms
            recompute()
        } catch {
            lastError = error.localizedDescription
        }
    }

    // MARK: - Logging

    /// Saves a day's flow and symptoms to HealthKit and updates local state.
    /// Passing `flow: nil` clears this app's flow entry for that day.
    func logDay(_ day: Date, flow: FlowLevel?, symptoms: Set<Symptom>) async {
        let dayStart = calendar.startOfDay(for: day)
        do {
            if let flow {
                // First day of a period if the previous day has no flow logged.
                // Backfilling an earlier day can leave the next day's metadata
                // stale; the grouping in CyclePredictor does not rely on it.
                let previousDay = calendar.date(byAdding: .day, value: -1, to: dayStart)
                let isCycleStart = previousDay.map { flowByDay[$0] == nil } ?? true
                try await manager.saveFlow(flow, on: dayStart, cycleStart: isCycleStart)
                flowByDay[dayStart] = flow
            } else {
                try await manager.deleteFlow(on: dayStart)
                // If the flow came from another app, it will reappear on the
                // next refresh — HealthKit only lets us delete our own samples.
                flowByDay.removeValue(forKey: dayStart)
            }
            await manager.saveSymptoms(symptoms, on: dayStart)
            if symptoms.isEmpty {
                symptomsByDay.removeValue(forKey: dayStart)
            } else {
                symptomsByDay[dayStart] = symptoms
            }
            recompute()
        } catch {
            lastError = error.localizedDescription
        }
    }

    private func recompute() {
        cycles = CyclePredictor.cycles(fromFlowDays: Array(flowByDay.keys), calendar: calendar)
        prediction = CyclePredictor.prediction(from: cycles, today: Date(), calendar: calendar)
    }

    // MARK: - Derived state

    var lastPeriodStart: Date? {
        cycles.last?.start
    }

    /// 1-based day within the current cycle, or `nil` before any data exists.
    var currentCycleDay: Int? {
        guard let start = lastPeriodStart else { return nil }
        let days = calendar.dateComponents([.day], from: start, to: calendar.startOfDay(for: Date())).day ?? 0
        return days >= 0 ? days + 1 : nil
    }

    /// Days until the predicted next period. Negative means the period is late.
    var daysUntilNextPeriod: Int? {
        guard let next = prediction?.nextPeriodStart else { return nil }
        return calendar.dateComponents([.day], from: calendar.startOfDay(for: Date()), to: next).day
    }

    var currentPhase: CyclePhase? {
        guard lastPeriodStart != nil else { return nil }
        let today = calendar.startOfDay(for: Date())
        if flowByDay[today] != nil { return .menstrual }
        guard let prediction, let ovulation = prediction.ovulationDate else { return .follicular }
        if let window = prediction.fertileWindow, window.contains(today) { return .ovulatory }
        return today < ovulation ? .follicular : .luteal
    }

    /// Marker for a calendar day: real flow first, then predictions.
    func mark(for date: Date) -> DayMark? {
        let day = calendar.startOfDay(for: date)
        if let flow = flowByDay[day] { return .period(flow) }
        guard let prediction else { return nil }
        if let ovulation = prediction.ovulationDate, calendar.isDate(day, inSameDayAs: ovulation) {
            return .ovulation
        }
        if let window = prediction.fertileWindow, window.contains(day) {
            return .fertile
        }
        for interval in prediction.upcomingPeriods where interval.contains(day) {
            return .predictedPeriod
        }
        return nil
    }

    func symptoms(on date: Date) -> Set<Symptom> {
        symptomsByDay[calendar.startOfDay(for: date)] ?? []
    }

    func flow(on date: Date) -> FlowLevel? {
        flowByDay[calendar.startOfDay(for: date)]
    }
}
