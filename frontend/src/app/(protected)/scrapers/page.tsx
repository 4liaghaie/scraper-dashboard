"use client";
import { ScrapeRunner, type ParamDef } from "@/components/scrape-runner";

export default function ScrapersPage() {
  const rebaidDetailsFields: ParamDef[] = [
    {
      type: "boolean",
      name: "missing_only",
      label: "Missing only",
      default: true,
    },
    {
      type: "number",
      name: "limit",
      label: "Limit",
      default: 300,
      min: 1,
      max: 5000,
      step: 1,
    },
    {
      type: "number",
      name: "timeout_ms",
      label: "Timeout (ms)",
      default: 12000,
      min: 1000,
      step: 500,
    },
  ];

  const rebatekeyDetailsFields: ParamDef[] = [
    {
      type: "boolean",
      name: "missing_only",
      label: "Missing only",
      default: true,
    },
    {
      type: "number",
      name: "limit",
      label: "Limit",
      default: 300,
      min: 1,
      max: 5000,
      step: 1,
    },
    {
      type: "number",
      name: "concurrency",
      label: "Concurrency",
      default: 12,
      min: 1,
      max: 64,
      step: 1,
    },
    {
      type: "number",
      name: "retries",
      label: "Retries",
      default: 2,
      min: 0,
      max: 10,
      step: 1,
    },
    {
      type: "number",
      name: "timeout",
      label: "Timeout (s)",
      default: 20,
      min: 5,
      max: 60,
      step: 1,
    },
  ];

  const amazonStoresFields: ParamDef[] = [
    {
      type: "text",
      name: "site",
      label: "Site (optional)",
      placeholder: "rebaid | myvipon | rebatekey",
    },
    {
      type: "boolean",
      name: "missing_only",
      label: "Missing only",
      default: true,
    },
    {
      type: "number",
      name: "limit",
      label: "Limit",
      default: 500,
      min: 1,
      max: 5000,
      step: 1,
    },
    {
      type: "number",
      name: "timeout_ms",
      label: "Timeout (ms)",
      default: 12000,
      min: 1000,
      step: 500,
    },
  ];

  const rebaidUrlsFields: ParamDef[] = [
    {
      type: "number",
      name: "max_pages",
      label: "Max pages (0 = all)",
      default: 0,
      min: 0,
      step: 1,
    },
    {
      type: "number",
      name: "timeout_ms",
      label: "Timeout (ms)",
      default: 30000,
      min: 5000,
      step: 500,
    },
    {
      type: "number",
      name: "delay_min",
      label: "Delay min (s)",
      default: 0.15,
      min: 0,
      step: 0.05,
    },
    {
      type: "number",
      name: "delay_max",
      label: "Delay max (s)",
      default: 0.45,
      min: 0,
      step: 0.05,
    },
  ];

  const rebatekeyUrlsFields: ParamDef[] = [
    {
      type: "boolean",
      name: "headed",
      label: "Show browser (headed)",
      default: false,
    },
  ];

  const myviponUrlsFields: ParamDef[] = [
    {
      type: "boolean",
      name: "headed",
      label: "Show browser (headed)",
      default: true,
    },
  ];

  const myviponDetailsFields: ParamDef[] = [
    {
      type: "boolean",
      name: "only_missing",
      label: "Only missing",
      default: true,
    },
    {
      type: "number",
      name: "limit",
      label: "Limit",
      default: 200,
      min: 1,
      max: 5000,
      step: 1,
    },
    {
      type: "number",
      name: "workers",
      label: "Workers (threads)",
      default: 6,
      min: 1,
      max: 32,
      step: 1,
    },
    {
      type: "number",
      name: "timeout",
      label: "Timeout (s)",
      default: 30,
      min: 5,
      max: 120,
      step: 1,
    },
    {
      type: "number",
      name: "retries",
      label: "Retries",
      default: 2,
      min: 0,
      max: 10,
      step: 1,
    },
    {
      type: "number",
      name: "backoff",
      label: "Backoff (s)",
      default: 1.0,
      min: 0,
      step: 0.25,
    },
  ];

  return (
    <main className="p-6 grid gap-4 md:grid-cols-2">
      <ScrapeRunner
        title="Full Fresh Run (URLs → Details → Stores)"
        kind="full_fresh_run"
        params={{
          // URL collection knobs
          rebaid_max_pages: 0,
          rebaid_timeout_ms: 30000,
          rebaid_delay_min: 0.15,
          rebaid_delay_max: 0.45,
          rebatekey_headed: false,
          myvipon_headed: true,

          // details knobs
          rebaid_detail_timeout_ms: 12000,
          rebatekey_concurrency: 12,
          rebatekey_retries: 2,
          rebatekey_timeout: 20.0,
          myvipon_workers: 6,
          myvipon_timeout: 30,

          // store enrichment knobs
          store_batch: 25,
          store_timeout_ms: 12000,
        }}
      />

      <ScrapeRunner
        title="Rebaid: Details"
        kind="rebaid_details"
        paramDefs={rebaidDetailsFields}
      />
      <ScrapeRunner
        title="RebateKey: Details"
        kind="rebatekey_details"
        paramDefs={rebatekeyDetailsFields}
      />
      <ScrapeRunner
        title="Amazon: Fill Store Fields"
        kind="amazon_stores"
        paramDefs={amazonStoresFields}
      />
      <ScrapeRunner
        title="Rebaid: Collect URLs"
        kind="rebaid_urls"
        paramDefs={rebaidUrlsFields}
      />
      <ScrapeRunner
        title="RebateKey: Collect URLs"
        kind="rebatekey_urls"
        paramDefs={rebatekeyUrlsFields}
      />
      <ScrapeRunner
        title="MyVipon: Collect URLs"
        kind="myvipon_urls"
        paramDefs={myviponUrlsFields}
      />
      <ScrapeRunner
        title="MyVipon: Details"
        kind="myvipon_details"
        paramDefs={myviponDetailsFields}
      />
    </main>
  );
}
