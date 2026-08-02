import Foundation

/// Flow intensity for a logged period day.
///
/// Raw values match HealthKit's menstrual flow category values
/// (`HKCategoryValueMenstrualFlow` / `HKCategoryValueVaginalBleeding`),
/// so a `FlowLevel` can be stored and read without further mapping.
/// Value 5 ("none") is intentionally absent: clearing a day deletes the
/// sample instead, and samples with value 5 are ignored when reading.
enum FlowLevel: Int, CaseIterable, Identifiable, Hashable {
    case unspecified = 1
    case light = 2
    case medium = 3
    case heavy = 4

    var id: Int { rawValue }

    var displayName: String {
        switch self {
        case .unspecified: return "Unspecified"
        case .light: return "Light"
        case .medium: return "Medium"
        case .heavy: return "Heavy"
        }
    }
}

/// Symptoms the app can log. Each case maps to a HealthKit category type
/// (see `HealthKitManager`).
enum Symptom: String, CaseIterable, Identifiable, Hashable {
    case abdominalCramps
    case headache
    case bloating
    case fatigue
    case moodChanges
    case breastPain
    case lowerBackPain
    case nausea
    case acne

    var id: String { rawValue }

    var displayName: String {
        switch self {
        case .abdominalCramps: return "Cramps"
        case .headache: return "Headache"
        case .bloating: return "Bloating"
        case .fatigue: return "Fatigue"
        case .moodChanges: return "Mood changes"
        case .breastPain: return "Breast pain"
        case .lowerBackPain: return "Lower back pain"
        case .nausea: return "Nausea"
        case .acne: return "Acne"
        }
    }

    var systemImage: String {
        switch self {
        case .abdominalCramps: return "bolt.heart"
        case .headache: return "brain.head.profile"
        case .bloating: return "wind"
        case .fatigue: return "zzz"
        case .moodChanges: return "theatermasks"
        case .breastPain: return "heart.circle"
        case .lowerBackPain: return "figure.walk"
        case .nausea: return "tornado"
        case .acne: return "face.dashed"
        }
    }
}

/// One menstrual cycle derived from logged flow days.
struct Cycle: Identifiable, Hashable {
    /// First day of the period that starts this cycle (start of day).
    let start: Date
    /// Number of days with logged flow at the start of the cycle.
    let periodLength: Int
    /// Days from this cycle's start to the next cycle's start.
    /// `nil` for the most recent (still ongoing) cycle.
    let cycleLength: Int?

    var id: Date { start }
}

/// The four classic cycle phases, used for the Today screen.
enum CyclePhase: String {
    case menstrual = "Menstrual"
    case follicular = "Follicular"
    case ovulatory = "Ovulatory"
    case luteal = "Luteal"

    var blurb: String {
        switch self {
        case .menstrual: return "Your period is here. Energy is often at its lowest — be kind to yourself."
        case .follicular: return "Estrogen is rising. Many people feel their energy and mood climb."
        case .ovulatory: return "You are in your estimated fertile window, around ovulation."
        case .luteal: return "Progesterone rises after ovulation. PMS symptoms can appear late in this phase."
        }
    }

    var systemImage: String {
        switch self {
        case .menstrual: return "drop.fill"
        case .follicular: return "leaf.fill"
        case .ovulatory: return "sun.max.fill"
        case .luteal: return "moon.fill"
        }
    }
}

/// Forward-looking estimates computed from cycle history.
struct Prediction {
    let averageCycleLength: Int
    let averagePeriodLength: Int
    let nextPeriodStart: Date
    /// The next few predicted periods as whole-day intervals.
    let upcomingPeriods: [DateInterval]
    /// Estimated ovulation day (about 14 days before the next period).
    let ovulationDate: Date?
    /// Estimated fertile window (5 days before ovulation through 1 day after).
    let fertileWindow: DateInterval?
}
