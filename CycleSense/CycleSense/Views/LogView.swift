import SwiftUI

/// Sheet for logging (or editing) one day's flow and symptoms.
struct LogView: View {
    @EnvironmentObject private var store: CycleStore
    @Environment(\.dismiss) private var dismiss

    let date: Date

    @State private var flow: FlowLevel?
    @State private var symptoms: Set<Symptom> = []
    @State private var isSaving = false
    @State private var hasLoaded = false

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    HStack(spacing: 8) {
                        ForEach(FlowLevel.allCases) { level in
                            flowButton(level)
                        }
                    }
                    .listRowInsets(EdgeInsets(top: 8, leading: 8, bottom: 8, trailing: 8))
                } header: {
                    Text("Menstrual flow")
                } footer: {
                    Text("Tap a selected level again to clear it. Clearing removes entries created by CycleSense; entries from other apps can be deleted in the Health app.")
                }

                Section("Symptoms") {
                    LazyVGrid(columns: [GridItem(.adaptive(minimum: 150), spacing: 8)], spacing: 8) {
                        ForEach(Symptom.allCases) { symptom in
                            symptomChip(symptom)
                        }
                    }
                    .listRowInsets(EdgeInsets(top: 8, leading: 8, bottom: 8, trailing: 8))
                }
            }
            .navigationTitle(date.formatted(date: .abbreviated, time: .omitted))
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { save() }
                        .disabled(isSaving)
                }
            }
            .onAppear(perform: loadExisting)
        }
    }

    private func flowButton(_ level: FlowLevel) -> some View {
        let isSelected = flow == level
        return Button {
            flow = isSelected ? nil : level
        } label: {
            VStack(spacing: 4) {
                Image(systemName: "drop.fill")
                    .imageScale(level == .heavy ? .large : .medium)
                Text(level.displayName)
                    .font(.caption2)
                    .lineLimit(1)
                    .minimumScaleFactor(0.7)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, 10)
            .background(
                isSelected ? Color.red : Color(.systemGray5),
                in: RoundedRectangle(cornerRadius: 10)
            )
            .foregroundStyle(isSelected ? .white : .primary)
        }
        .buttonStyle(.plain)
    }

    private func symptomChip(_ symptom: Symptom) -> some View {
        let isSelected = symptoms.contains(symptom)
        return Button {
            if isSelected {
                symptoms.remove(symptom)
            } else {
                symptoms.insert(symptom)
            }
        } label: {
            HStack(spacing: 6) {
                Image(systemName: symptom.systemImage)
                    .imageScale(.small)
                Text(symptom.displayName)
                    .font(.footnote)
                    .lineLimit(1)
                    .minimumScaleFactor(0.8)
                Spacer(minLength: 0)
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .background(
                isSelected ? Color.pink.opacity(0.85) : Color(.systemGray5),
                in: Capsule()
            )
            .foregroundStyle(isSelected ? .white : .primary)
        }
        .buttonStyle(.plain)
    }

    private func loadExisting() {
        guard !hasLoaded else { return }
        flow = store.flow(on: date)
        symptoms = store.symptoms(on: date)
        hasLoaded = true
    }

    private func save() {
        isSaving = true
        Task {
            await store.logDay(date, flow: flow, symptoms: symptoms)
            isSaving = false
            dismiss()
        }
    }
}

#Preview {
    LogView(date: Date())
        .environmentObject(CycleStore())
}
