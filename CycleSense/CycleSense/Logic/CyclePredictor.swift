import Foundation

/// Pure functions that turn logged flow days into cycles and predictions.
/// Everything here is deterministic and side-effect free, so it can be
/// unit-tested without HealthKit.
enum CyclePredictor {
    /// Used until enough history exists to compute real averages.
    static let defaultCycleLength = 28
    static let defaultPeriodLength = 5
    /// A single un-logged day inside a period is bridged into one episode.
    static let maxGapWithinPeriod = 1
    /// Number of recent cycles used for averaging.
    static let historyWindow = 6

    /// Groups logged flow days into cycles, oldest first.
    static func cycles(fromFlowDays flowDays: [Date], calendar: Calendar = .current) -> [Cycle] {
        let days = Set(flowDays.map { calendar.startOfDay(for: $0) }).sorted()
        guard let firstDay = days.first else { return [] }

        // Group consecutive flow days (allowing a small gap) into period episodes.
        var episodes: [[Date]] = []
        var current: [Date] = [firstDay]
        for day in days.dropFirst() {
            let gap = calendar.dateComponents([.day], from: current[current.count - 1], to: day).day ?? 0
            if gap <= maxGapWithinPeriod + 1 {
                current.append(day)
            } else {
                episodes.append(current)
                current = [day]
            }
        }
        episodes.append(current)

        // A cycle runs from the start of one episode to the start of the next.
        var result: [Cycle] = []
        for (index, episode) in episodes.enumerated() {
            let start = episode[0]
            let periodLength = (calendar.dateComponents([.day], from: start, to: episode[episode.count - 1]).day ?? 0) + 1
            var cycleLength: Int?
            if index + 1 < episodes.count {
                cycleLength = calendar.dateComponents([.day], from: start, to: episodes[index + 1][0]).day
            }
            result.append(Cycle(start: start, periodLength: periodLength, cycleLength: cycleLength))
        }
        return result
    }

    /// Computes averages and forward-looking estimates from cycle history.
    /// Returns `nil` when no period has been logged yet.
    static func prediction(from cycles: [Cycle], today: Date = Date(), calendar: Calendar = .current) -> Prediction? {
        guard let lastCycle = cycles.last else { return nil }

        // Average over recent cycles, ignoring implausible lengths so one
        // data glitch or long tracking gap does not skew the estimate.
        let completedLengths = cycles.compactMap(\.cycleLength).suffix(historyWindow).filter { (15...60).contains($0) }
        let averageCycleLength = completedLengths.isEmpty
            ? defaultCycleLength
            : Int((Double(completedLengths.reduce(0, +)) / Double(completedLengths.count)).rounded())

        let periodLengths = cycles.suffix(historyWindow).map(\.periodLength).filter { (1...10).contains($0) }
        let averagePeriodLength = periodLengths.isEmpty
            ? defaultPeriodLength
            : Int((Double(periodLengths.reduce(0, +)) / Double(periodLengths.count)).rounded())

        // Next period = last period start + average cycle length. If tracking
        // lapsed and that date is more than a full cycle in the past, roll
        // forward so the calendar stays useful; a slightly-past date is kept
        // so the UI can show "N days late".
        let todayStart = calendar.startOfDay(for: today)
        var nextPeriodStart = calendar.date(byAdding: .day, value: averageCycleLength, to: lastCycle.start) ?? todayStart
        while (calendar.dateComponents([.day], from: nextPeriodStart, to: todayStart).day ?? 0) > averageCycleLength {
            guard let rolled = calendar.date(byAdding: .day, value: averageCycleLength, to: nextPeriodStart) else { break }
            nextPeriodStart = rolled
        }

        var upcoming: [DateInterval] = []
        var periodStart = nextPeriodStart
        for _ in 0..<3 {
            if let periodEnd = calendar.date(byAdding: .day, value: averagePeriodLength - 1, to: periodStart) {
                upcoming.append(DateInterval(start: periodStart, end: periodEnd))
            }
            guard let next = calendar.date(byAdding: .day, value: averageCycleLength, to: periodStart) else { break }
            periodStart = next
        }

        // Ovulation ~14 days before the next period; fertile window spans the
        // 5 days before ovulation through the day after.
        let ovulationDate = calendar.date(byAdding: .day, value: -14, to: nextPeriodStart)
        var fertileWindow: DateInterval?
        if let ovulationDate,
           let windowStart = calendar.date(byAdding: .day, value: -5, to: ovulationDate),
           let windowEnd = calendar.date(byAdding: .day, value: 1, to: ovulationDate) {
            fertileWindow = DateInterval(start: windowStart, end: windowEnd)
        }

        return Prediction(
            averageCycleLength: averageCycleLength,
            averagePeriodLength: averagePeriodLength,
            nextPeriodStart: nextPeriodStart,
            upcomingPeriods: upcoming,
            ovulationDate: ovulationDate,
            fertileWindow: fertileWindow
        )
    }
}
