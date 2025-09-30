"use client";

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

type HealthResp = { status: string };

export default function HealthCard() {
  const query = useQuery({
    queryKey: ["health"],
    queryFn: async (): Promise<HealthResp> => {
      const { data } = await api.get<HealthResp>("/health");
      return data;
    },
    refetchOnWindowFocus: false,
  });

  return (
    <Card className="max-w-md">
      <CardHeader>
        <CardTitle>API Health</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {query.isLoading && <p>Checking…</p>}
        {query.isError && <p className="text-red-600">Failed to reach API.</p>}
        {query.isSuccess && (
          <p className="font-mono">status: {query.data.status}</p>
        )}
        <div className="flex gap-2">
          <Button onClick={() => query.refetch()} disabled={query.isFetching}>
            {query.isFetching ? "Refreshing…" : "Refresh"}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
