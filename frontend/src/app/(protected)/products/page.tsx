// src/app/(protected)/products/page.tsx
"use client";
import { Suspense } from "react";

import { useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
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
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Sheet,
  SheetTrigger,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
  SheetFooter,
  SheetClose,
} from "@/components/ui/sheet";
import { ExternalLink, Info, Loader2 } from "lucide-react";
import ProductDetailsDialog, {
  Product,
} from "@/components/product-details-dialog";
export const dynamic = "force-dynamic";

type ProductPage = {
  items: Product[];
  total: number;
  page: number;
  page_size: number;
  has_next: boolean;
  has_prev: boolean;
};

const SITES = [
  { label: "All", value: "all" },
  { label: "myvipon", value: "myvipon" },
  { label: "rebaid", value: "rebaid" },
  { label: "rebatekey", value: "rebatekey" },
];

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
  return (
    <Suspense fallback={<main className="p-6">Loading…</main>}>
      <ProductsPageInner />
    </Suspense>
  );
}
function ProductsPageInner() {
  const router = useRouter();
  const pathname = usePathname();
  const search = useSearchParams();

  const initialSite = search.get("site") ?? "";
  const [site, setSite] = useState<string>(initialSite);
  const [siteTab, setSiteTab] = useState<string>(initialSite || "all");

  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);

  const [qInput, setQInput] = useState("");
  const [q, setQ] = useState("");
  const [sort, setSort] = useState("-created_at");

  const [store, setStore] = useState<"any" | "present" | "missing">("any");

  // Filters
  const [lastSeenFrom, setLastSeenFrom] = useState<string>("");
  const [lastSeenTo, setLastSeenTo] = useState<string>("");

  // Export drawer state & form
  const [exportOpen, setExportOpen] = useState(false);
  const [exportingSheet, setExportingSheet] = useState(false);
  const [sheetId, setSheetId] = useState("");
  const [worksheet, setWorksheet] = useState("Products Export");
  const [sheetMode, setSheetMode] = useState<"replace" | "append">("replace");

  // details modal
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [selected, setSelected] = useState<Product | null>(null);

  // sync site from URL on client nav
  useEffect(() => {
    const s = search.get("site") ?? "";
    setSite(s);
    setSiteTab(s || "all");
  }, [search]);

  // reset page when filters change
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
    if (lastSeenFrom) p.last_seen_from = lastSeenFrom;
    if (lastSeenTo) p.last_seen_to = lastSeenTo;
    return p;
  }, [page, pageSize, site, q, sort, store, lastSeenFrom, lastSeenTo]);

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

  // Tabs change → set site and update URL (?site=)
  const onTabChange = (val: string) => {
    setSiteTab(val);
    const newSite = val === "all" ? "" : val;
    setSite(newSite);

    const qs = new URLSearchParams(search.toString());
    if (newSite) qs.set("site", newSite);
    else qs.delete("site");
    router.replace(`${pathname}${qs.toString() ? `?${qs.toString()}` : ""}`, {
      scroll: false,
    });
  };

  // CSV export
  const apiBase = process.env.NEXT_PUBLIC_API_BASE ?? "";
  const handleExportCsv = () => {
    const qs = new URLSearchParams();
    if (site) qs.set("site", site);
    if (lastSeenFrom) qs.set("last_seen_from", lastSeenFrom);
    if (lastSeenTo) qs.set("last_seen_to", lastSeenTo);
    const query = qs.toString();
    const url = `${apiBase}/exports/products.csv${query ? `?${query}` : ""}`;
    window.open(url, "_blank");
  };

  // Google Sheets export
  const handleExportSheet = async () => {
    if (exportingSheet) return;
    if (!sheetId.trim()) {
      alert("Enter Spreadsheet ID");
      return;
    }
    const qs = new URLSearchParams();
    if (site) qs.set("site", site);
    if (lastSeenFrom) qs.set("last_seen_from", lastSeenFrom);
    if (lastSeenTo) qs.set("last_seen_to", lastSeenTo);

    try {
      setExportingSheet(true);
      await api.post(`/exports/products.google-sheet?${qs.toString()}`, {
        spreadsheet_id: sheetId.trim(),
        worksheet: worksheet.trim() || undefined,
        mode: sheetMode,
        start_cell: "A1",
      });
      alert("Exported to Google Sheet ✅");
    } catch (err: any) {
      const msg =
        err?.response?.data?.detail ||
        err?.message ||
        "Export failed. Check permissions and Spreadsheet ID.";
      alert(`Export failed: ${msg}`);
    } finally {
      setExportingSheet(false);
    }
  };

  return (
    <main className="p-6">
      <Card className="max-w-[1600px] mx-auto">
        <CardHeader className="space-y-4">
          <div className="flex items-center justify-between gap-3">
            <CardTitle className="text-xl">Products</CardTitle>

            {/* Export + Refresh */}
            <div className="flex items-center gap-2">
              <Sheet open={exportOpen} onOpenChange={setExportOpen}>
                <SheetTrigger asChild>
                  <Button variant="secondary">Export…</Button>
                </SheetTrigger>
                <SheetContent
                  side="right"
                  className="
    w-[100vw] max-w-[100vw] sm:w-[640px]
    h-[100dvh] sm:h-auto
    overflow-y-auto
    p-4 sm:p-6
  "
                >
                  <SheetHeader>
                    <SheetTitle>Export products</SheetTitle>
                    <SheetDescription>
                      Exports respect your current filters (site tab, last seen
                      range, search).
                    </SheetDescription>
                  </SheetHeader>

                  <div className="mt-6 space-y-8">
                    {/* CSV */}
                    <section className="space-y-3">
                      <h3 className="text-sm font-medium">CSV</h3>
                      <p className="text-xs text-muted-foreground">
                        Download a CSV file with the current filters.
                      </p>
                      <div>
                        <Button
                          onClick={handleExportCsv}
                          className="w-full"
                          variant="secondary"
                        >
                          Download CSV
                        </Button>
                      </div>
                    </section>

                    {/* Google Sheets */}
                    <section className="space-y-3">
                      <h3 className="text-sm font-medium">Google Sheets</h3>
                      <div className="grid grid-cols-1 gap-3">
                        <div>
                          <label className="block text-xs mb-1">
                            Spreadsheet ID
                          </label>
                          <Input
                            placeholder="1_OpkZ4F5ybI9dKWL5ZwngFhNV7iUodLw8W1HMaElir8"
                            value={sheetId}
                            onChange={(e) => setSheetId(e.target.value)}
                          />
                        </div>
                        <div>
                          <label className="block text-xs mb-1">
                            Worksheet title
                          </label>
                          <Input
                            placeholder="Products Export"
                            value={worksheet}
                            onChange={(e) => setWorksheet(e.target.value)}
                          />
                        </div>
                        <div>
                          <label className="block text-xs mb-1">Mode</label>
                          <select
                            value={sheetMode}
                            onChange={(e) =>
                              setSheetMode(
                                e.target.value as "replace" | "append"
                              )
                            }
                            className="w-full border rounded-md px-3 py-2 bg-background"
                          >
                            <option value="replace">replace</option>
                            <option value="append">append</option>
                          </select>
                        </div>

                        <div className="flex flex-col sm:flex-row gap-2">
                          <Button
                            onClick={handleExportSheet}
                            disabled={exportingSheet}
                            className="w-full sm:w-auto"
                          >
                            {exportingSheet ? (
                              <>
                                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                Exporting…
                              </>
                            ) : (
                              "Export to Google Sheet"
                            )}
                          </Button>
                          <SheetClose asChild>
                            <Button
                              variant="outline"
                              className="w-full sm:w-auto"
                            >
                              Close
                            </Button>
                          </SheetClose>
                        </div>

                        <p className="text-xs text-muted-foreground">
                          Make sure the spreadsheet is shared with your service
                          account (Editor).
                        </p>
                      </div>
                    </section>

                    {/* Current filters summary */}
                    <section className="text-xs text-muted-foreground">
                      <div>
                        <span className="font-medium">Using:</span>{" "}
                        {site ? `site=${site}` : "All sites"}
                        {lastSeenFrom || lastSeenTo
                          ? ` • last_seen=${lastSeenFrom || "…"} → ${
                              lastSeenTo || "…"
                            }`
                          : ""}
                        {q ? ` • search="${q}"` : ""}
                      </div>
                    </section>
                  </div>

                  {/* Optional pinned footer area (keeps actions visible on small screens) */}
                  <SheetFooter className="sticky bottom-0 bg-background pt-4 mt-6">
                    {/* You can add extra actions here if needed */}
                  </SheetFooter>
                </SheetContent>
              </Sheet>

              <Button
                variant="outline"
                onClick={() => refetch()}
                disabled={isFetching}
              >
                {isFetching ? "Refreshing…" : "Refresh"}
              </Button>
            </div>
          </div>

          {/* Site Tabs */}
          <Tabs value={siteTab} onValueChange={onTabChange} className="w-full">
            <TabsList className="grid grid-cols-4 w-full md:w-auto">
              {SITES.map((s) => (
                <TabsTrigger key={s.value} value={s.value}>
                  {s.label}
                </TabsTrigger>
              ))}
            </TabsList>
          </Tabs>

          {/* Filters row (no export controls here anymore) */}
          <div className="grid grid-cols-1 md:grid-cols-12 gap-3 mt-2">
            {/* Search */}
            <div className="md:col-span-5">
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

            {/* Page size */}
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

            {/* Date range */}
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

      <ProductDetailsDialog
        open={detailsOpen}
        onOpenChange={setDetailsOpen}
        product={selected}
      />
    </main>
  );
}
