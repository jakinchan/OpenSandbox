<script setup lang="ts">
import DefaultTheme from "vitepress/theme";
import { useData, useRouter, withBase } from "vitepress";
import { onMounted, watch } from "vue";

const { Layout } = DefaultTheme;
const { page } = useData();
const router = useRouter();

// The #1090 refactor removed the Chinese i18n locale and the legacy
// `overview/*` / `*/readme` URL scheme. Old bookmarks and search results
// still point at those paths, so map them to the new structure on 404.
// Anything we can't map falls back to the home page.
const EXACT: Record<string, string> = {
  "/overview/home": "/",
  "/overview/architecture": "/architecture/",
  "/overview/credential-vault": "/guides/credential-vault",
  "/overview/release-verification": "/community/release-verification",
  "/design/single-host-network": "/architecture/single-host-network",
  "/kubernetes/development": "/kubernetes/deployment",
};

function resolveLegacy(rawPath: string): string {
  let p = rawPath.replace(/\.html$/, "").replace(/\/$/, "").toLowerCase();
  p = p.replace(/^\/zh(?=\/|$)/, "");
  if (p === "" || p === "/") return "/";
  if (EXACT[p]) return EXACT[p];
  if (p.startsWith("/oseps/")) return "/community/oseps";
  const stripped = p.replace(/\/(readme|development)$/, "");
  if (stripped !== p) return stripped;
  return "/";
}

function maybeRedirect() {
  if (!page.value.isNotFound) return;
  const target = resolveLegacy(window.location.pathname);
  router.go(withBase(target));
}

onMounted(maybeRedirect);
watch(() => page.value.isNotFound, maybeRedirect);
</script>

<template>
  <Layout />
</template>
