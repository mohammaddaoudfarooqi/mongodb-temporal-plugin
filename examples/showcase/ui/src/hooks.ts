import { useEffect, useRef, useState } from "react";
import { parseRoute, type Route } from "./lib";

/** The current route; follows the URL hash. */
export function useRoute(): Route {
  const [route, setRoute] = useState(() => parseRoute(window.location.hash));
  useEffect(() => {
    const on = () => setRoute(parseRoute(window.location.hash));
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  return route;
}

/**
 * Loads with fn, again every `every` ms while mounted, and on reload().
 * A poll is skipped while the previous one is still in flight.
 */
export function usePoll<T>(fn: () => Promise<T>, every: number, deps: unknown[] = []) {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<string>();
  const [tick, setTick] = useState(0);
  const fnRef = useRef(fn);
  fnRef.current = fn;
  useEffect(() => {
    let live = true;
    let busy = false;
    const load = () => {
      if (busy) return;
      busy = true;
      fnRef.current()
        .then(
          (d) => live && (setData(d), setError(undefined)),
          (e: Error) => live && setError(e.message),
        )
        .finally(() => (busy = false));
    };
    load();
    const t = window.setInterval(load, every);
    return () => {
      live = false;
      window.clearInterval(t);
    };
  }, [...deps, tick, every]);
  return { data, error, reload: () => setTick((n) => n + 1) };
}
