"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { ExternalLink } from "lucide-react";

export type SiteBrief = { id: number; name: string };

export type Product = {
  id: number;
  site: SiteBrief;
  product_url: string;
  title: string | null;
  price: string | number | null;
  image_url: string | null;
  description: string | null;
  category: string | null;
  type?: string | null;
  amazon_url: string | null;
  amazon_store_url: string | null;
  amazon_store_name: string | null;
  external_id: string | null;
  first_seen_at: string;
  last_seen_at: string;
  created_at: string;
  updated_at: string;
};

function fmtDate(iso?: string) {
  return iso
    ? new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
      }).format(new Date(iso))
    : "-";
}

function fmtPrice(v: Product["price"]) {
  if (v === null || v === undefined || v === "") return "-";
  if (typeof v === "number") return `$${v.toFixed(2)}`;
  if (/^\s*\$/.test(String(v))) return String(v);
  const m = String(v).match(/[\d,.]+/);
  return m ? `$${m[0]}` : String(v);
}

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  product: Product | null;
};

type RefreshDiag = {
  antibot_hits: number;
  timeouts: number;
  http_errors: number;
  no_store_found: number;
};

async function refreshAmazonStore(productId: number) {
  const res = await fetch(
    `${process.env.NEXT_PUBLIC_API_BASE}/products/${productId}/refresh-amazon-store`,
    {
      method: "POST",
    }
  );
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data?.detail || "Failed to refresh Amazon store");
  }
  return data as {
    product_id: number;
    amazon_url: string;
    found: boolean;
    amazon_store_name: string | null;
    amazon_store_url: string | null;
    updated: boolean;
    antibot_hits: number;
    timeouts: number;
    http_errors: number;
    no_store_found: number;
  };
}

export default function ProductDetailsDialog({
  open,
  onOpenChange,
  product,
}: Props) {
  // local copy so we can update the store fields inline after refresh
  const [p, setP] = useState<Product | null>(product);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [diag, setDiag] = useState<RefreshDiag | null>(null);

  useEffect(() => setP(product), [product]);

  const handleRefresh = async () => {
    if (!p) return;
    setLoading(true);
    setErr(null);
    setDiag(null);
    try {
      const data = await refreshAmazonStore(p.id);
      // update local copy if we found anything
      if (data.amazon_store_name || data.amazon_store_url) {
        setP((prev) =>
          prev
            ? {
                ...prev,
                amazon_store_name:
                  data.amazon_store_name ?? prev.amazon_store_name,
                amazon_store_url:
                  data.amazon_store_url ?? prev.amazon_store_url,
                updated_at: new Date().toISOString(),
              }
            : prev
        );
      }
      setDiag({
        antibot_hits: data.antibot_hits,
        timeouts: data.timeouts,
        http_errors: data.http_errors,
        no_store_found: data.no_store_found,
      });
    } catch (e: any) {
      setErr(e.message || "Unknown error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-5xl md:max-w-6xl">
        <DialogHeader>
          <DialogTitle className="pr-10">
            {p?.title || "Product details"}
          </DialogTitle>
          <DialogDescription className="truncate">
            {p?.product_url}
          </DialogDescription>
        </DialogHeader>

        {p && (
          <div className="grid grid-cols-1 md:grid-cols-[320px_1fr] gap-6">
            {/* Image */}
            <div className="border rounded-md overflow-hidden bg-muted/30 flex items-center justify-center">
              {p.image_url ? (
                // eslint-disable-next-line @next/next/no-img-element
                <img
                  src={p.image_url}
                  alt={p.title ?? "Product image"}
                  className="object-contain w-full h-[320px] bg-white"
                />
              ) : (
                <div className="text-sm text-muted-foreground py-20">
                  No image
                </div>
              )}
            </div>

            {/* Fields */}
            <div className="space-y-3">
              <div className="text-sm text-muted-foreground">
                <span className="mr-2">Site:</span>
                <span className="font-medium">{p.site?.name ?? "—"}</span>
              </div>

              <div className="text-sm text-muted-foreground">
                <span className="mr-2">Category:</span>
                <span className="font-medium">{p.category ?? "—"}</span>
              </div>

              <div className="text-sm text-muted-foreground">
                <span className="mr-2">Type:</span>
                <span className="font-medium uppercase">{p.type ?? "—"}</span>
              </div>

              <div className="text-sm text-muted-foreground">
                <span className="mr-2">Price:</span>
                <span className="font-medium">{fmtPrice(p.price)}</span>
              </div>

              <div className="text-sm text-muted-foreground">
                <span className="mr-2">Created:</span>
                <span className="font-medium">{fmtDate(p.created_at)}</span>
              </div>

              <div className="text-sm text-muted-foreground">
                <span className="mr-2">Updated:</span>
                <span className="font-medium">{fmtDate(p.updated_at)}</span>
              </div>

              {/* Store info + Refresh */}
              <div className="text-sm text-muted-foreground">
                <div className="flex flex-wrap items-center gap-2">
                  <div>
                    <span className="mr-2">Store:</span>
                    <span className="font-medium">
                      {p.amazon_store_name || "—"}
                    </span>
                    {p.amazon_store_url && (
                      <Button
                        asChild
                        variant="link"
                        size="sm"
                        className="h-auto p-0 ml-2 align-baseline"
                      >
                        <a
                          href={p.amazon_store_url}
                          target="_blank"
                          rel="noreferrer"
                        >
                          Store page{" "}
                          <ExternalLink className="ml-1 h-3.5 w-3.5" />
                        </a>
                      </Button>
                    )}
                  </div>

                  <Button
                    variant="secondary"
                    size="sm"
                    onClick={handleRefresh}
                    disabled={loading || !p.amazon_url}
                    title={
                      p.amazon_url
                        ? "Scrape Amazon store for this product"
                        : "No amazon_url available"
                    }
                  >
                    {loading ? "Refreshing…" : "Refresh Amazon Store"}
                  </Button>

                  {err && (
                    <span className="text-red-600 text-xs ml-1">{err}</span>
                  )}
                </div>

                {diag && (
                  <div className="mt-1 text-xs text-muted-foreground">
                    antibot: {diag.antibot_hits} · timeouts: {diag.timeouts} ·
                    http errors: {diag.http_errors} · no store found:{" "}
                    {diag.no_store_found}
                  </div>
                )}
              </div>

              <div className="pt-2">
                <div className="text-sm font-medium mb-1">Description</div>
                <div className="text-sm max-h-48 overflow-auto whitespace-pre-wrap leading-relaxed">
                  {p.description || "—"}
                </div>
              </div>

              <div className="flex flex-wrap gap-2 pt-2">
                <Button asChild variant="outline" size="sm">
                  <a href={p.product_url} target="_blank" rel="noreferrer">
                    Product <ExternalLink className="ml-1 h-4 w-4" />
                  </a>
                </Button>
                {p.amazon_url && (
                  <Button asChild variant="outline" size="sm">
                    <a href={p.amazon_url} target="_blank" rel="noreferrer">
                      Amazon <ExternalLink className="ml-1 h-4 w-4" />
                    </a>
                  </Button>
                )}
              </div>
            </div>
          </div>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
