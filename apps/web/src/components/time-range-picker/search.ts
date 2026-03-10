import type { TimeRange } from "./types"

export function applyTimeRangeSearch<T extends Record<string, unknown>>(
  prev: T,
  range: TimeRange,
) {
  return {
    ...prev,
    startTime: range.startTime,
    endTime: range.endTime,
    timePreset: range.presetValue,
  }
}
