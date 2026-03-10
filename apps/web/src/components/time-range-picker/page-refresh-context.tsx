import * as React from "react"

import { relativeToAbsolute } from "@/lib/time-utils"

export const LIVE_REFRESH_INTERVAL_MS = 10_000

export interface RelativeRefreshRange {
  startTime: string
  endTime: string
  presetValue: string
}

interface PageRefreshContextValue {
  refreshVersion: number
  liveEnabled: boolean
  setLiveEnabled: React.Dispatch<React.SetStateAction<boolean>>
  reload: () => void
}

interface PageRefreshProviderProps {
  children: React.ReactNode
  timePreset?: string
  onRelativeRangeRefresh?: (range: RelativeRefreshRange) => void
}

const PageRefreshContext = React.createContext<PageRefreshContextValue | null>(null)

export function resolveRelativeRefreshRange(timePreset?: string): RelativeRefreshRange | null {
  if (!timePreset) return null

  const range = relativeToAbsolute(timePreset)
  if (!range) return null

  return {
    ...range,
    presetValue: timePreset,
  }
}

export function isDocumentVisible(doc: Document = document): boolean {
  return doc.visibilityState === "visible"
}

export function PageRefreshProvider({
  children,
  timePreset,
  onRelativeRangeRefresh,
}: PageRefreshProviderProps) {
  const [refreshVersion, setRefreshVersion] = React.useState(0)
  const [liveEnabled, setLiveEnabled] = React.useState(false)
  const [isVisible, setIsVisible] = React.useState(() =>
    typeof document === "undefined" ? true : isDocumentVisible(document),
  )

  const triggerReload = React.useEffectEvent(() => {
    const relativeRange = resolveRelativeRefreshRange(timePreset)
    if (relativeRange) {
      onRelativeRangeRefresh?.(relativeRange)
    }
    setRefreshVersion((current) => current + 1)
  })

  React.useEffect(() => {
    if (typeof document === "undefined") return

    const handleVisibilityChange = () => {
      const visible = isDocumentVisible(document)
      setIsVisible(visible)

      if (visible && liveEnabled) {
        triggerReload()
      }
    }

    document.addEventListener("visibilitychange", handleVisibilityChange)
    return () => document.removeEventListener("visibilitychange", handleVisibilityChange)
  }, [liveEnabled, triggerReload])

  React.useEffect(() => {
    if (!liveEnabled || !isVisible) return

    const intervalId = window.setInterval(() => {
      triggerReload()
    }, LIVE_REFRESH_INTERVAL_MS)

    return () => window.clearInterval(intervalId)
  }, [isVisible, liveEnabled, triggerReload])

  const value = React.useMemo<PageRefreshContextValue>(
    () => ({
      refreshVersion,
      liveEnabled,
      setLiveEnabled,
      reload: () => triggerReload(),
    }),
    [liveEnabled, refreshVersion, triggerReload],
  )

  return (
    <PageRefreshContext.Provider value={value}>
      {children}
    </PageRefreshContext.Provider>
  )
}

export function usePageRefreshContext() {
  const context = React.useContext(PageRefreshContext)
  if (!context) {
    throw new Error("usePageRefreshContext must be used within a PageRefreshProvider")
  }
  return context
}

export function useOptionalPageRefreshContext() {
  return React.useContext(PageRefreshContext)
}
