"use client";

import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, setAuthToken } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

type Me = { id: number; email: string; role: string; is_active: boolean };
type ProfileResponse = { user: Me; access_token?: string; token_type?: string };

export default function ProfilePage() {
  const qc = useQueryClient();

  // Load current user
  const { data: me } = useQuery<Me>({
    queryKey: ["me"],
    queryFn: async () => (await api.get("/profile")).data,
    refetchOnWindowFocus: false,
  });

  // ----- Profile (email) -----
  const [email, setEmail] = useState("");
  const [saveErr, setSaveErr] = useState<string | null>(null);
  const saveProfile = useMutation({
    mutationFn: async () => {
      const { data } = await api.patch<ProfileResponse>("/profile", {
        email: email || undefined,
      });
      return data;
    },
    onSuccess: (res) => {
      if (res.access_token) setAuthToken(res.access_token); // rotate token on email change
      qc.invalidateQueries({ queryKey: ["me"] });
      setSaveErr(null);
    },
    onError: (e: any) =>
      setSaveErr(e?.response?.data?.detail ?? "Failed to save profile"),
  });

  // ----- Password change -----
  const [curPw, setCurPw] = useState("");
  const [newPw, setNewPw] = useState("");
  const [newPw2, setNewPw2] = useState("");
  const [pwErr, setPwErr] = useState<string | null>(null);
  const changePassword = useMutation({
    mutationFn: async () => {
      if (newPw !== newPw2)
        throw { response: { data: { detail: "Passwords do not match" } } };
      const { data } = await api.post<ProfileResponse>("/profile/password", {
        current_password: curPw,
        new_password: newPw,
        new_password_confirm: newPw2,
      });
      return data;
    },
    onSuccess: (res) => {
      if (res.access_token) setAuthToken(res.access_token); // rotate token
      setCurPw("");
      setNewPw("");
      setNewPw2("");
      setPwErr(null);
    },
    onError: (e: any) =>
      setPwErr(e?.response?.data?.detail ?? "Failed to change password"),
  });

  // Prefill email once profile loads
  const currentEmail = me?.email ?? "";
  const shownEmail = email === "" ? currentEmail : email;

  return (
    <main className="p-6">
      <div className="mx-auto grid max-w-3xl gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Profile</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div>
              <label className="block text-sm mb-1">Email</label>
              <Input
                type="email"
                value={shownEmail}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@example.com"
              />
            </div>
            {saveErr && <p className="text-sm text-red-600">{saveErr}</p>}
            <Button
              onClick={() => saveProfile.mutate()}
              disabled={saveProfile.isPending}
            >
              {saveProfile.isPending ? "Saving…" : "Save changes"}
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Change password</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div>
              <label className="block text-sm mb-1">Current password</label>
              <Input
                type="password"
                value={curPw}
                onChange={(e) => setCurPw(e.target.value)}
              />
            </div>
            <div>
              <label className="block text-sm mb-1">New password</label>
              <Input
                type="password"
                value={newPw}
                onChange={(e) => setNewPw(e.target.value)}
              />
            </div>
            <div>
              <label className="block text-sm mb-1">Confirm new password</label>
              <Input
                type="password"
                value={newPw2}
                onChange={(e) => setNewPw2(e.target.value)}
              />
            </div>
            {pwErr && <p className="text-sm text-red-600">{pwErr}</p>}
            <Button
              onClick={() => changePassword.mutate()}
              disabled={changePassword.isPending}
              variant="outline"
            >
              {changePassword.isPending ? "Changing…" : "Change password"}
            </Button>
          </CardContent>
        </Card>
      </div>
    </main>
  );
}
