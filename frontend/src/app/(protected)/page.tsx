"use client";

import * as React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  PieChart,
  Pie,
  Tooltip,
  Legend,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  LineChart,
  Line,
  ResponsiveContainer,
  Cell, // ⬅️ add Cell
} from "recharts";
import { Button } from "@/components/ui/button";

type BySite = { site: string; count: number };
type StoreInfo = {
  total: number;
  with_amazon_url: number;
  with_store_info: number;
  missing_store_info: number;
};
type DailyNew = { day: string; count: number };
type JobStatus = { status: string; count: number };

const API = process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, "") || "";

// Theme-driven palette (matches your shadcn tokens)
const THEME_COLORS = [
  "hsl(var(--chart-1))",
  "hsl(var(--chart-2))",
  "hsl(var(--chart-3))",
  "hsl(var(--chart-4))",
  "hsl(var(--chart-5))",
];
// Dark grayscale palette (shades of black)
const GRAYS = ["#0f0f0f", "#2a2a2a", "#666666", "#707070", "#7a7a7a"];

// Optional: fixed colors per site
const SITE_COLOR: Record<string, string> = {
  rebaid: "hsl(var(--chart-1))",
  rebatekey: "hsl(var(--chart-2))",
  myvipon: "hsl(var(--chart-3))",
};

// Optional: fixed colors for coverage pie
const COVERAGE_COLOR: Record<string, string> = {
  "With store info": "hsl(var(--chart-2))",
  "Missing store info": "hsl(var(--chart-5))",
};

async function fetchJSON<T>(path: string): Promise<T> {
  const r = await fetch(`${API}${path}`, { cache: "no-store" });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}

export default function DashboardPage() {
  const [bySite, setBySite] = React.useState<BySite[]>([]);
  const [storeInfo, setStoreInfo] = React.useState<StoreInfo | null>(null);
  const [dailyNew, setDailyNew] = React.useState<DailyNew[]>([]);
  const [jobStatuses, setJobStatuses] = React.useState<JobStatus[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  const load = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [bs, si, dn, js] = await Promise.all([
        fetchJSON<BySite[]>("/metrics/products/by-site"),
        fetchJSON<StoreInfo>("/metrics/products/store-info"),
        fetchJSON<DailyNew[]>("/metrics/products/daily-new?days=14"),
        fetchJSON<JobStatus[]>("/metrics/jobs/status-counts"),
      ]);
      setBySite(bs);
      setStoreInfo(si);
      setDailyNew(dn);
      setJobStatuses(js);
    } catch (e: any) {
      setError(e?.message || "Failed to load");
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    load();
  }, [load]);

  const storeCoverage = storeInfo
    ? [
        { label: "With store info", value: storeInfo.with_store_info },
        { label: "Missing store info", value: storeInfo.missing_store_info },
      ]
    : [];

  return (
    <div className="p-4 md:p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
        <div className="flex items-center gap-2">
          <Button variant="outline" onClick={load} disabled={loading}>
            {loading ? "Refreshing..." : "Refresh"}
          </Button>
        </div>
      </div>

      {error && (
        <Card className="border-destructive">
          <CardHeader>
            <CardTitle className="text-destructive">Error</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm">{error}</p>
          </CardContent>
        </Card>
      )}

      {/* Top summary */}
      {storeInfo && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <StatCard title="Total products" value={storeInfo.total} />
          <StatCard title="With Amazon URL" value={storeInfo.with_amazon_url} />
          <StatCard
            title="Store info filled"
            value={storeInfo.with_store_info}
          />
          <StatCard
            title="Store info missing"
            value={storeInfo.missing_store_info}
          />
        </div>
      )}

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Products by Site</CardTitle>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={bySite}
                  dataKey="count"
                  nameKey="site"
                  cx="50%"
                  cy="50%"
                  outerRadius={100}
                  label={(d: any) => `${d.site} (${d.count})`}
                >
                  {bySite.map((d, i) => (
                    <Cell
                      key={d.site}
                      fill={GRAYS[i % GRAYS.length]}
                      stroke="hsl(var(--background))"
                      strokeWidth={2}
                    />
                  ))}
                </Pie>
                <Tooltip />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Store Info Coverage</CardTitle>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={storeCoverage}
                  dataKey="value"
                  nameKey="label"
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={100}
                  label={(d: any) => `${d.label} (${d.value})`}
                >
                  {storeCoverage.map((d, i) => (
                    <Cell
                      key={d.label}
                      fill={GRAYS[i % GRAYS.length]}
                      stroke="hsl(var(--background))"
                      strokeWidth={2}
                    />
                  ))}
                </Pie>
                <Tooltip />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>New Products (last 14 days)</CardTitle>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={dailyNew}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="day"
                  tickFormatter={(d) => new Date(d).toLocaleDateString()}
                />
                <YAxis allowDecimals={false} />
                <Tooltip
                  labelFormatter={(d) =>
                    new Date(d as string).toLocaleDateString()
                  }
                />
                <Line type="monotone" dataKey="count" dot />
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Job Runs by Status</CardTitle>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={jobStatuses}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="status" />
                <YAxis allowDecimals={false} />
                <Tooltip />
                <Bar dataKey="count" />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function StatCard({ title, value }: { title: string; value: number }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm text-muted-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{value.toLocaleString()}</div>
      </CardContent>
    </Card>
  );
}
