import { tone } from "../lib";

export function Badge({ value }: { value?: string }) {
  if (!value) return null;
  return <span className={`badge ${tone(value)}`}>{value}</span>;
}
