// src/app/(protected)/products/page.tsx
"use client";

import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ExternalLink, Info } from "lucide-react";
import ProductDetailsDialog, {
  Product,
} from "@/components/product-details-dialog";

type ProductPage = {
  items: Product[];
  total: number;
  page: number;
  page_size: number;
  has_next: boolean;
  has_prev: boolean;
};

const fmtDate = (iso?: string) =>
  iso
    ? new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
      }).format(new Date(iso))
    : "-";

const fmtPrice = (v: Product["price"]) => {
  if (v === null || v === undefined || v === "") return "-";
  if (typeof v === "number") return `$${v.toFixed(2)}`;
  if (/^\s*\$/.test(String(v))) return String(v);
  const m = String(v).match(/[\d,.]+/);
  return m ? `$${m[0]}` : String(v);
};

export default function ProductsPage() {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [site, setSite] = useState<string>("");

  const [qInput, setQInput] = useState("");
  const [q, setQ] = useState("");
  const [sort, setSort] = useState("-created_at");

  // NEW: store presence filter
  const [store, setStore] = useState<"any" | "present" | "missing">("any");

  // NEW: export filters
  const [lastSeenFrom, setLastSeenFrom] = useState<string>(""); // YYYY-MM-DD
  const [lastSeenTo, setLastSeenTo] = useState<string>(""); // YYYY-MM-DD
  const [idsCsv, setIdsCsv] = useState<string>(""); // "1,2,3"

  // details modal state
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [selected, setSelected] = useState<Product | null>(null);

  // When filters change, reset to first page
  useEffect(() => {
    setPage(1);
  }, [site, q, sort, pageSize, store, lastSeenFrom, lastSeenTo]);

  const params = useMemo(() => {
    const p: Record<string, string | number> = {
      page,
      page_size: pageSize,
      sort,
    };
    if (site) p.site = site;
    if (q) p.q = q;
    if (store !== "any") p.store = store;
    if (lastSeenFrom) p.last_seen_from = lastSeenFrom; // ← added
    if (lastSeenTo) p.last_seen_to = lastSeenTo; // ← added
    return p;
  }, [page, pageSize, site, q, sort, store, lastSeenFrom, lastSeenTo]); // ← added

  const { data, isLoading, isError, refetch, isFetching } =
    useQuery<ProductPage>({
      queryKey: ["products", params] as const,
      queryFn: async (): Promise<ProductPage> =>
        (await api.get("/products", { params })).data,
      placeholderData: (prev) => prev,
      refetchOnWindowFocus: false,
    });

  const pageData: ProductPage = data ?? {
    items: [],
    total: 0,
    page,
    page_size: pageSize,
    has_next: false,
    has_prev: false,
  };

  const onSearch = () => setQ(qInput.trim());
  const toggleSort = (field: "created_at" | "last_seen_at" | "price") => {
    setSort((curr) => (curr === field ? `-${field}` : field));
  };
  const openDetails = (p: Product) => {
    setSelected(p);
    setDetailsOpen(true);
  };

  // === NEW: Export handler ===
  const apiBase = process.env.NEXT_PUBLIC_API_BASE ?? ""; // e.g. "http://localhost:8000"
  const handleExportCsv = () => {
    const qs = new URLSearchParams();
    if (site) qs.set("site", site);
    if (lastSeenFrom) qs.set("last_seen_from", lastSeenFrom);
    if (lastSeenTo) qs.set("last_seen_to", lastSeenTo);

    const trimmed = idsCsv.trim();
    // Single numeric id -> hit /exports/products/{id}.csv for nicer filename
    if (trimmed && /^\d+$/.test(trimmed)) {
      const url = `${apiBase}/exports/products/${trimmed}.csv`;
      window.open(url, "_blank");
      return;
    }
    // Multiple ids -> /exports/products.csv?ids=...
    if (trimmed) {
      qs.set("ids", trimmed);
    }

    const query = qs.toString();
    const url = `${apiBase}/exports/products.csv` + (query ? `?${query}` : "");
    window.open(url, "_blank");
  };

  return (
    <main className="p-6">
      <Card className="max-w-[1600px] mx-auto">
        <CardHeader className="space-y-4">
          <CardTitle className="text-xl">Products</CardTitle>

          <div className="grid grid-cols-1 md:grid-cols-12 gap-3">
            <div className="md:col-span-3">
              <label className="block text-sm mb-1">Site</label>
              <select
                value={site}
                onChange={(e) => setSite(e.target.value)}
                className="w-full border rounded-md px-3 py-2 bg-background"
              >
                <option value="">All</option>
                <option value="myvipon">myvipon</option>
                <option value="rebaid">rebaid</option>
                <option value="rebatekey">rebatekey</option>
              </select>
            </div>

            <div className="md:col-span-4">
              <label className="block text-sm mb-1">Search</label>
              <div className="flex gap-2">
                <Input
                  placeholder="title, url, store, category…"
                  value={qInput}
                  onChange={(e) => setQInput(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && onSearch()}
                />
                <Button onClick={onSearch} disabled={isFetching}>
                  Search
                </Button>
              </div>
            </div>

            <div className="md:col-span-2">
              <label className="block text-sm mb-1">Page size</label>
              <select
                value={pageSize}
                onChange={(e) => setPageSize(Number(e.target.value))}
                className="w-full border rounded-md px-3 py-2 bg-background"
              >
                {[25, 50, 100, 200].map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </div>

            {/* === NEW: Export controls === */}
            <div className="md:col-span-2">
              <label className="block text-sm mb-1">Last seen (from)</label>
              <Input
                type="date"
                value={lastSeenFrom}
                onChange={(e) => setLastSeenFrom(e.target.value)}
              />
            </div>
            <div className="md:col-span-2">
              <label className="block text-sm mb-1">Last seen (to)</label>
              <Input
                type="date"
                value={lastSeenTo}
                onChange={(e) => setLastSeenTo(e.target.value)}
              />
            </div>
            <div className="md:col-span-12 flex items-end justify-end gap-2">
              <Button
                variant="secondary"
                onClick={handleExportCsv}
                title="Download CSV (filters above applied)"
              >
                Export CSV
              </Button>
              <Button
                variant="outline"
                onClick={() => refetch()}
                disabled={isFetching}
              >
                {isFetching ? "Refreshing…" : "Refresh"}
              </Button>
            </div>
            {/* === END: Export controls === */}
          </div>
        </CardHeader>

        <CardContent>
          {isLoading && <p>Loading…</p>}
          {isError && <p className="text-red-600">Failed to load products.</p>}

          <>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-16">ID</TableHead>
                    <TableHead>Site</TableHead>
                    <TableHead className="w-20">Product</TableHead>
                    <TableHead>Title</TableHead>
                    <TableHead>Category</TableHead>
                    <TableHead>Type</TableHead>
                    {/* NEW: Store column */}
                    <TableHead>Store</TableHead>
                    <TableHead
                      className="cursor-pointer select-none"
                      onClick={() => toggleSort("price")}
                    >
                      Price{" "}
                      {sort.includes("price")
                        ? sort.startsWith("-")
                          ? "↓"
                          : "↑"
                        : ""}
                    </TableHead>
                    <TableHead
                      className="cursor-pointer select-none"
                      onClick={() => toggleSort("created_at")}
                    >
                      Created{" "}
                      {sort.includes("created_at")
                        ? sort.startsWith("-")
                          ? "↓"
                          : "↑"
                        : ""}
                    </TableHead>
                    <TableHead
                      className="cursor-pointer select-none"
                      onClick={() => toggleSort("last_seen_at")}
                    >
                      Last seen{" "}
                      {sort.includes("last_seen_at")
                        ? sort.startsWith("-")
                          ? "↓"
                          : "↑"
                        : ""}
                    </TableHead>
                    <TableHead className="w-32">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pageData.items.map((p) => (
                    <TableRow key={p.id}>
                      <TableCell className="font-mono">{p.id}</TableCell>

                      <TableCell>
                        <span className="inline-flex items-center rounded-full bg-muted px-2 py-0.5 text-xs">
                          {p.site?.name ?? "-"}
                        </span>
                      </TableCell>

                      {/* Minimal URL icon with hover showing full URL */}
                      <TableCell>
                        <Button
                          asChild
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8"
                          title={p.product_url}
                          aria-label="Open product"
                        >
                          <a
                            href={p.product_url}
                            target="_blank"
                            rel="noreferrer"
                          >
                            <ExternalLink className="h-4 w-4" />
                          </a>
                        </Button>
                      </TableCell>

                      {/* Truncated title (hover full title) */}
                      <TableCell title={p.title ?? ""}>
                        <div className="max-w-[340px] truncate">
                          {p.title ?? "—"}
                        </div>
                      </TableCell>

                      <TableCell title={p.category ?? ""}>
                        <div className="max-w-[180px] truncate">
                          {p.category ?? "—"}
                        </div>
                      </TableCell>

                      <TableCell>
                        <span className="uppercase text-xs text-muted-foreground">
                          {p.type ?? "—"}
                        </span>
                      </TableCell>

                      {/* NEW: Store cell */}
                      <TableCell title={p.amazon_store_url ?? ""}>
                        <div className="max-w-[240px] truncate">
                          {p.amazon_store_name ? (
                            <a
                              href={p.amazon_store_url ?? "#"}
                              target="_blank"
                              rel="noreferrer"
                              className="underline"
                            >
                              {p.amazon_store_name}
                            </a>
                          ) : (
                            "—"
                          )}
                        </div>
                      </TableCell>

                      <TableCell>{fmtPrice(p.price)}</TableCell>
                      <TableCell>{fmtDate(p.created_at)}</TableCell>
                      <TableCell>{fmtDate(p.last_seen_at)}</TableCell>

                      <TableCell>
                        <div className="flex gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => openDetails(p)}
                          >
                            <Info className="mr-1 h-4 w-4" />
                            Details
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>

            <div className="mt-4 flex items-center justify-between">
              <div className="text-sm text-muted-foreground">
                Total: <span className="font-medium">{pageData.total}</span>
              </div>
            </div>

            <div className="mt-2 flex gap-2 justify-end">
              <Button
                variant="outline"
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={!pageData.has_prev || isFetching}
              >
                Prev
              </Button>
              <span className="px-2 py-2 text-sm">
                Page <span className="font-medium">{pageData.page}</span>
              </span>
              <Button
                variant="outline"
                onClick={() => setPage((p) => p + 1)}
                disabled={!pageData.has_next || isFetching}
              >
                Next
              </Button>
            </div>
          </>
        </CardContent>
      </Card>

      {/* Details dialog */}
      <ProductDetailsDialog
        open={detailsOpen}
        onOpenChange={setDetailsOpen}
        product={selected}
      />
    </main>
  );
}
