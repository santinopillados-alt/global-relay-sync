import { useState, useEffect, useRef, useCallback } from "react";

const WS_URL = "ws://localhost:8002/ws";
const API_URL = "http://localhost:8002";

export function useSync() {
  const [connected, setConnected] = useState(false);
  const [stats, setStats] = useState(null);
  const [conflicts, setConflicts] = useState([]);
  const [events, setEvents] = useState([]);
  const [sourceCount, setSourceCount] = useState(0);
  const [targetCount, setTargetCount] = useState(0);
  const wsRef = useRef(null);
  const reconnectTimer = useRef(null);
  const reconnectAttempts = useRef(0);

  const fetchCounts = useCallback(async () => {
    try {
      const [src, tgt] = await Promise.all([
        fetch(`${API_URL}/api/orders/source`).then(r => r.json()),
        fetch(`${API_URL}/api/orders/target`).then(r => r.json()),
      ]);
      setSourceCount(src.length);
      setTargetCount(tgt.length);
    } catch {}
  }, []);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      reconnectAttempts.current = 0;
      setConnected(true);
    };

    ws.onclose = () => {
      setConnected(false);
      const delay = Math.min(1000 * 2 ** reconnectAttempts.current, 15000);
      reconnectAttempts.current += 1;
      reconnectTimer.current = setTimeout(connect, delay);
    };

    ws.onerror = () => ws.close();

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === "ping") return;

        if (msg.type === "snapshot") {
          setStats(msg.data.stats);
          setConflicts(msg.data.conflicts || []);
          fetchCounts();
          return;
        }

        if (msg.type === "cdc_event") {
          setEvents(prev => [msg, ...prev].slice(0, 100));
          fetchCounts();
        }

        if (msg.type === "metric") {
          setStats(prev => ({ ...prev, ...msg.data }));
        }
      } catch {}
    };
  }, [fetchCounts]);

  useEffect(() => {
    connect();
    const interval = setInterval(async () => {
      try {
        const s = await fetch(`${API_URL}/api/stats`).then(r => r.json());
        setStats(s);
        fetchCounts();
      } catch {}
    }, 3000);

    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
      clearInterval(interval);
    };
  }, [connect, fetchCounts]);

  return { connected, stats, conflicts, events, sourceCount, targetCount };
}