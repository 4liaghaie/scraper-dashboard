"use client";

import { useState, useMemo } from "react";
import {
  useQuery,
  useMutation,
  useQueryClient,
  keepPreviousData,
} from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

type UserRow = { id: number; email: string; role: string; is_active: boolean };
type UserPage = {
  items: UserRow[];
  total: number;
  page: number;
  page_size: number;
};

export default function AdminUsersPage() {
  const qc = useQueryClient();
  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);

  const params = useMemo(
    () => ({ page, page_size: pageSize }),
    [page, pageSize]
  );

  const { data } = useQuery<UserPage>({
    queryKey: ["admin-users", params],
    queryFn: async () => (await api.get("/admin/users", { params })).data,
    placeholderData: keepPreviousData,
    refetchOnWindowFocus: false,
  });

  // create user form
  const [email, setEmail] = useState("");
  const [pw, setPw] = useState("");
  const [role, setRole] = useState<"viewer" | "admin" | "superuser">("viewer");
  const [err, setErr] = useState<string | null>(null);

  const createUser = useMutation({
    mutationFn: async () =>
      (await api.post("/admin/users", { email, password: pw, role }))
        .data as UserRow,
    onSuccess: () => {
      setEmail("");
      setPw("");
      setRole("viewer");
      setErr(null);
      qc.invalidateQueries({ queryKey: ["admin-users"] });
    },
    onError: (e: any) =>
      setErr(e?.response?.data?.detail ?? "Failed to create user"),
  });

  const pageData = data ?? { items: [], total: 0, page, page_size: pageSize };

  return (
    <main className="p-6">
      <Card className="max-w-[1000px] mx-auto">
        <CardHeader>
          <CardTitle>Users</CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          <form
            className="grid grid-cols-1 md:grid-cols-12 gap-3 items-end"
            onSubmit={(e) => {
              e.preventDefault();
              createUser.mutate();
            }}
          >
            <div className="md:col-span-4">
              <label className="text-sm">Email</label>
              <Input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
              />
            </div>
            <div className="md:col-span-4">
              <label className="text-sm">Password</label>
              <Input
                type="password"
                value={pw}
                onChange={(e) => setPw(e.target.value)}
                required
              />
            </div>
            <div className="md:col-span-2">
              <label className="text-sm">Role</label>
              <select
                value={role}
                onChange={(e) => setRole(e.target.value as any)}
                className="w-full border rounded-md px-3 py-2 bg-background"
              >
                <option value="viewer">viewer</option>
                <option value="admin">admin</option>
                <option value="superuser">superuser</option>
              </select>
            </div>
            <div className="md:col-span-2">
              <Button
                type="submit"
                disabled={createUser.isPending}
                className="w-full"
              >
                {createUser.isPending ? "Creating…" : "Create"}
              </Button>
            </div>
            {err && (
              <p className="text-red-600 text-sm md:col-span-12">{err}</p>
            )}
          </form>

          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Email</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Active</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pageData.items.map((u) => (
                  <TableRow key={u.id}>
                    <TableCell className="font-mono">{u.id}</TableCell>
                    <TableCell>{u.email}</TableCell>
                    <TableCell>{u.role}</TableCell>
                    <TableCell>{u.is_active ? "Yes" : "No"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          <div className="flex items-center justify-between">
            <div className="text-sm text-muted-foreground">
              Total: {pageData.total}
            </div>
            <div className="flex gap-2">
              <Button
                variant="outline"
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page <= 1}
              >
                Prev
              </Button>
              <span className="px-2 py-2 text-sm">Page {pageData.page}</span>
              <Button
                variant="outline"
                onClick={() => setPage((p) => p + 1)}
                disabled={pageData.items.length < pageSize}
              >
                Next
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </main>
  );
}
