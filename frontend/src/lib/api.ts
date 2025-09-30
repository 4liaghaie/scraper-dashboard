import axios from "axios";

export const api = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000",
  headers: { "Content-Type": "application/json" },
});

export function setAuthToken(token: string | null) {
  if (token) {
    api.defaults.headers.common["Authorization"] = `Bearer ${token}`;
    if (typeof window !== "undefined") {
      localStorage.setItem("token", token);
      // also set a cookie so Next middleware can read it
      document.cookie = `auth_token=${token}; Path=/; Max-Age=2592000; SameSite=Lax`;
    }
  } else {
    delete api.defaults.headers.common["Authorization"];
    if (typeof window !== "undefined") {
      localStorage.removeItem("token");
      // clear cookie
      document.cookie = "auth_token=; Path=/; Max-Age=0; SameSite=Lax";
    }
  }
}

// load token on client boot
if (typeof window !== "undefined") {
  const t = localStorage.getItem("token");
  if (t) setAuthToken(t);
}

// (optional) auto-logout on 401s
api.interceptors.response.use(
  (r) => r,
  (err) => {
    const status = err?.response?.status;
    if (typeof window !== "undefined" && status === 401) {
      const here = window.location.pathname + window.location.search;
      // 🚫 don't redirect if we're already on /login
      if (!here.startsWith("/login")) {
        const next = encodeURIComponent(here);
        window.location.href = `/login?next=${next}`;
      }
    }
    return Promise.reject(err);
  }
);
