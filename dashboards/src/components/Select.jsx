export default function Select({ id, label, value, options, onChange }) {
  return (
    <div className="filter">
      <label htmlFor={id}>{label}</label>
      <div className="select">
        <select id={id} value={value} onChange={(e) => onChange(e.target.value)}>
          {options.map((o) => (
            <option key={o} value={o}>{o}</option>
          ))}
        </select>
      </div>
    </div>
  )
}
