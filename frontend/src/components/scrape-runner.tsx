"use client";
import { useEffect, useMemo, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input"; // if you don't have these, swap to plain <input>
import { Label } from "@/components/ui/label";
import { api } from "@/lib/api";

type JobState = {
  id: string;
  kind: string;
  status: "queued" | "running" | "done" | "error" | "cancelled";
  total: number;
  done: number;
  ok: number;
  err: number;
  note: string;
  meta: Record<string, any>;
};

type BaseParam = { name: string; label: string; help?: string };
type NumberParam = BaseParam & {
  type: "number";
  min?: number;
  max?: number;
  step?: number;
  default?: number;
};
type BooleanParam = BaseParam & { type: "boolean"; default?: boolean };
type TextParam = BaseParam & {
  type: "text";
  placeholder?: string;
  default?: string;
};
type SelectParam = BaseParam & {
  type: "select";
  options: { label: string; value: string }[];
  default?: string;
};
export type ParamDef = NumberParam | BooleanParam | TextParam | SelectParam;

type Props = {
  title: string;
  kind: string; // backend job kind
  /** optional initial params (used to seed defaults) */
  params?: Record<string, any>;
  /** declarative field list to render inputs */
  paramDefs?: ParamDef[];
  startLabel?: string;
};

export function ScrapeRunner({
  title,
  kind,
  params,
  paramDefs,
  startLabel = "Run",
}: Props) {
  const [job, setJob] = useState<JobState | null>(null);
  const [running, setRunning] = useState(false);
  const esRef = useRef<EventSource | null>(null);

  // Build initial form from paramDefs + props.params + localStorage
  const defaults = useMemo(() => {
    const fromDefs =
      paramDefs?.reduce<Record<string, any>>((acc, d) => {
        if (d.type === "boolean")
          acc[d.name] = (d as BooleanParam).default ?? false;
        else if (d.type === "number")
          acc[d.name] = (d as NumberParam).default ?? 0;
        else if (d.type === "select")
          acc[d.name] =
            (d as SelectParam).default ??
            (d as SelectParam).options?.[0]?.value ??
            "";
        else acc[d.name] = (d as TextParam).default ?? "";
        return acc;
      }, {}) ?? {};
    return { ...fromDefs, ...(params ?? {}) };
  }, [paramDefs, params]);

  const [form, setForm] = useState<Record<string, any>>(() => {
    try {
      const saved = localStorage.getItem(`jobParams:${kind}`);
      return saved ? { ...defaults, ...JSON.parse(saved) } : defaults;
    } catch {
      return defaults;
    }
  });

  useEffect(() => {
    // persist whenever changed
    try {
      localStorage.setItem(`jobParams:${kind}`, JSON.stringify(form));
    } catch {}
  }, [form, kind]);

  const update = (name: string, value: any) =>
    setForm((f) => ({ ...f, [name]: value }));

  const reset = () => setForm(defaults);

  const start = async () => {
    setRunning(true);
    // POST job start with current form values
    const res = await api.post("/jobs/start/run", { kind, params: form });
    const { job_id } = res.data as { job_id: string };

    const es = new EventSource(
      `${process.env.NEXT_PUBLIC_API_BASE}/jobs/stream/${job_id}`,
      { withCredentials: true } as any // TS appeasement, EventSource has this in browsers
    );
    esRef.current = es;

    es.addEventListener("started", (ev) => {
      try {
        setJob(JSON.parse((ev as MessageEvent).data));
      } catch {}
    });
    es.addEventListener("progress", (ev) => {
      try {
        setJob(JSON.parse((ev as MessageEvent).data));
      } catch {}
    });
    es.addEventListener("done", (ev) => {
      try {
        setJob(JSON.parse((ev as MessageEvent).data));
      } catch {}
      es.close();
      setRunning(false);
    });
    es.addEventListener("error", (ev) => {
      try {
        setJob(JSON.parse((ev as MessageEvent).data));
      } catch {}
      es.close();
      setRunning(false);
    });
  };

  useEffect(
    () => () => {
      esRef.current?.close();
    },
    []
  );

  const pct =
    job && job.total > 0
      ? Math.min(100, Math.round((job.done / job.total) * 100))
      : running
      ? 5
      : 0;

  const renderField = (def: ParamDef) => {
    const val = form[def.name];
    if (def.type === "boolean") {
      return (
        <div key={def.name} className="flex items-center justify-between gap-2">
          <Label htmlFor={`${kind}-${def.name}`}>{def.label}</Label>
          <input
            id={`${kind}-${def.name}`}
            type="checkbox"
            checked={!!val}
            onChange={(e) => update(def.name, e.target.checked)}
          />
        </div>
      );
    }
    if (def.type === "number") {
      return (
        <div key={def.name} className="grid gap-1">
          <Label htmlFor={`${kind}-${def.name}`}>{def.label}</Label>
          <Input
            id={`${kind}-${def.name}`}
            type="number"
            value={val ?? ""}
            onChange={(e) =>
              update(
                def.name,
                e.target.value === "" ? "" : Number(e.target.value)
              )
            }
            min={(def as NumberParam).min}
            max={(def as NumberParam).max}
            step={(def as NumberParam).step}
          />
          {def.help ? (
            <p className="text-xs text-muted-foreground">{def.help}</p>
          ) : null}
        </div>
      );
    }
    if (def.type === "select") {
      const sel = def as SelectParam;
      return (
        <div key={def.name} className="grid gap-1">
          <Label htmlFor={`${kind}-${def.name}`}>{def.label}</Label>
          <select
            id={`${kind}-${def.name}`}
            className="h-9 rounded-md border px-2"
            value={val ?? ""}
            onChange={(e) => update(def.name, e.target.value)}
          >
            {sel.options.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
          {def.help ? (
            <p className="text-xs text-muted-foreground">{def.help}</p>
          ) : null}
        </div>
      );
    }
    // text
    const t = def as TextParam;
    return (
      <div key={def.name} className="grid gap-1">
        <Label htmlFor={`${kind}-${def.name}`}>{def.label}</Label>
        <Input
          id={`${kind}-${def.name}`}
          type="text"
          value={val ?? ""}
          onChange={(e) => update(def.name, e.target.value)}
          placeholder={t.placeholder}
        />
        {def.help ? (
          <p className="text-xs text-muted-foreground">{def.help}</p>
        ) : null}
      </div>
    );
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0">
        <CardTitle className="text-lg">{title}</CardTitle>
        <Badge
          variant={
            job?.status === "done"
              ? "default"
              : job?.status === "error"
              ? "destructive"
              : "secondary"
          }
        >
          {job?.status ?? "idle"}
        </Badge>
      </CardHeader>
      <CardContent className="space-y-3">
        {paramDefs?.length ? (
          <div className="grid gap-3">
            {paramDefs.map(renderField)}
            <div className="flex items-center gap-2">
              <Button
                variant="secondary"
                type="button"
                onClick={reset}
                disabled={running}
              >
                Reset
              </Button>
            </div>
          </div>
        ) : null}

        <div className="flex items-center gap-2 pt-1">
          <Button onClick={start} disabled={running}>
            {running ? "Running…" : startLabel}
          </Button>
          {job?.total ? (
            <div className="text-sm text-muted-foreground">
              {job.done}/{job.total} (ok {job.ok}, err {job.err})
            </div>
          ) : null}
        </div>

        <Progress value={pct} />
        {job?.note ? (
          <div className="text-xs text-muted-foreground">note: {job.note}</div>
        ) : null}
        {job?.meta?.last_url ? (
          <div className="text-xs break-all">last: {job.meta.last_url}</div>
        ) : null}
      </CardContent>
    </Card>
  );
}
