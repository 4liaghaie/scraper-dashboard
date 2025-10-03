"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { api, setAuthToken } from "@/lib/api";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { LogOut, User, Users } from "lucide-react";

type Me = {
  id: number;
  email: string;
  role: "viewer" | "admin" | "superuser" | string;
  is_active: boolean;
  avatar_url?: string | null;
};

const SITE_LINKS = [
  { label: "All products", href: "/products" },
  { label: "myvipon", href: "/products?site=myvipon" },
  { label: "rebaid", href: "/products?site=rebaid" },
  { label: "rebatekey", href: "/products?site=rebatekey" },
];

export default function Navbar() {
  const pathname = usePathname();
  const router = useRouter();
  const [me, setMe] = useState<Me | null>(null);
  const [loading, setLoading] = useState(true);
  const [mobileOpen, setMobileOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const { data } = await api.get<Me>("/auth/me");
        if (!cancelled) setMe(data);
      } catch {
        if (!cancelled) setMe(null);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const isSuperUser = me?.role === "superuser";

  // remove old /products top-level; we'll use a dropdown instead
  const items = [
    { href: "/", label: "Dashboard" },
    { href: "/scrapers", label: "Scrapers" },
    ...(me && me.role === "admin"
      ? [{ href: "/admin/users", label: "Users" }]
      : []),
  ];

  const isActive = (href: string) =>
    pathname === href || (href !== "/" && pathname?.startsWith(href + "/"));

  const onLogout = () => {
    setAuthToken(null);
    setMe(null);
    router.push("/login");
  };

  return (
    <header className="sticky top-0 z-40 w-full border-b bg-background/80 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-screen-2xl items-center gap-3 px-4">
        {/* Brand */}
        <Link href="/" className="font-semibold">
          Scraper Dashboard
        </Link>

        {/* Desktop nav */}
        <nav className="ml-4 hidden items-center gap-1 md:flex">
          {items.map((it) => (
            <Link
              key={it.href}
              href={it.href}
              className={
                "rounded-md px-3 py-1.5 text-sm " +
                (isActive(it.href)
                  ? "bg-muted font-medium"
                  : "hover:bg-muted/60")
              }
            >
              {it.label}
            </Link>
          ))}

          {/* Products dropdown */}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="px-3 py-1.5 text-sm">
                Products
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="w-48">
              <DropdownMenuLabel>Products</DropdownMenuLabel>
              <DropdownMenuSeparator />
              {SITE_LINKS.map((link) => (
                <DropdownMenuItem key={link.href} asChild>
                  <Link href={link.href}>{link.label}</Link>
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        </nav>

        {/* Mobile menu button */}
        <button
          className="ml-auto inline-flex items-center rounded-md border px-3 py-1.5 text-sm md:hidden"
          onClick={() => setMobileOpen((v) => !v)}
          aria-label="Toggle menu"
        >
          Menu
        </button>

        {/* Right side (desktop) */}
        <div className="ml-auto hidden items-center gap-3 md:flex">
          {!loading && me ? (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  className="rounded-full"
                  aria-label="User menu"
                >
                  <Avatar className="h-8 w-8">
                    <AvatarImage
                      src={me.avatar_url ?? undefined}
                      alt={me.email}
                    />
                    <AvatarFallback>
                      {(me.email?.[0] ?? "?").toUpperCase()}
                    </AvatarFallback>
                  </Avatar>
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-56">
                <DropdownMenuLabel>
                  <div className="truncate text-sm font-medium">{me.email}</div>
                  <div className="text-xs uppercase text-muted-foreground">
                    {me.role}
                  </div>
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={() => router.push("/profile")}>
                  <User className="mr-2 h-4 w-4" />
                  Profile
                </DropdownMenuItem>
                {isSuperUser && (
                  <DropdownMenuItem onClick={() => router.push("/admin/users")}>
                    <Users className="mr-2 h-4 w-4" />
                    Users
                  </DropdownMenuItem>
                )}
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={onLogout}>
                  <LogOut className="mr-2 h-4 w-4" />
                  Logout
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          ) : (
            <div />
          )}
        </div>
      </div>

      {/* Mobile drawer */}
      {mobileOpen && (
        <div className="border-t bg-background md:hidden">
          <nav className="flex flex-col p-2">
            {items.map((it) => (
              <Link
                key={it.href}
                href={it.href}
                onClick={() => setMobileOpen(false)}
                className={
                  "rounded-md px-3 py-2 text-sm " +
                  (isActive(it.href)
                    ? "bg-muted font-medium"
                    : "hover:bg-muted/60")
                }
              >
                {it.label}
              </Link>
            ))}
            {/* Products submenu on mobile */}
            <div className="mt-2 border-t pt-2">
              <div className="px-3 pb-1 text-xs font-medium text-muted-foreground">
                Products
              </div>
              {SITE_LINKS.map((l) => (
                <Link
                  key={l.href}
                  href={l.href}
                  onClick={() => setMobileOpen(false)}
                  className="rounded-md px-3 py-2 text-sm hover:bg-muted/60"
                >
                  {l.label}
                </Link>
              ))}
            </div>

            <div className="mt-2 border-t pt-2">
              {me ? (
                <div className="flex items-center justify-between px-2">
                  <span className="text-sm text-muted-foreground">
                    {me.email?.split("@")[0] ?? "user"} • {me.role}
                  </span>
                  <div className="flex gap-2">
                    <Button variant="outline" size="sm" asChild>
                      <Link
                        href="/profile"
                        onClick={() => setMobileOpen(false)}
                      >
                        Profile
                      </Link>
                    </Button>
                    {isSuperUser && (
                      <Button variant="outline" size="sm" asChild>
                        <Link
                          href="/admin/users"
                          onClick={() => setMobileOpen(false)}
                        >
                          Users
                        </Link>
                      </Button>
                    )}
                    <Button variant="outline" size="sm" onClick={onLogout}>
                      Logout
                    </Button>
                  </div>
                </div>
              ) : (
                <Button variant="outline" size="sm" asChild className="mx-2">
                  <Link href="/login" onClick={() => setMobileOpen(false)}>
                    Login
                  </Link>
                </Button>
              )}
            </div>
          </nav>
        </div>
      )}
    </header>
  );
}
