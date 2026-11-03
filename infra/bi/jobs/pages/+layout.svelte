<!--
	Evidence's default layout plus the ADR-0007 design floor fixes its stock
	components need (checked with architecture-docs/design-floor/check.mjs,
	config in design-floor.json). Keep this the only place they live.
-->
<script>
	import '@evidence-dev/tailwind/fonts.css';
	import '../app.css';
	import { onMount } from 'svelte';
	import { EvidenceDefaultLayout } from '@evidence-dev/core-components';
	export let data;

	// Stock components render a few elements the floor rejects; fix them as
	// they appear (pages render client-side and re-render on navigation).
	function patch(root) {
		// Skip-link target: the layout's own <main>, after the header and sidebar.
		const main = root.querySelector('main');
		if (main && main.id !== 'main-content') {
			main.id = 'main-content';
			main.setAttribute('tabindex', '-1');
		}
		// Scrollable tables must be reachable by keyboard.
		for (const box of root.querySelectorAll('.scrollbox:not([tabindex])')) {
			box.setAttribute('tabindex', '0');
			box.setAttribute('role', 'region');
			box.setAttribute('aria-label', 'Table, scrolls sideways');
		}
		// The dropdown trigger opens a dialog, not a listbox: role=combobox
		// without aria-controls is invalid, a plain button with aria-haspopup is not.
		for (const button of root.querySelectorAll('button[role="combobox"][aria-haspopup="dialog"]')) {
			button.removeAttribute('role');
		}
	}

	// Rendered only in the browser: <main> gets its id from patch(), and the
	// prerender step would reject a link to an id it cannot see.
	let mounted = false;

	onMount(() => {
		patch(document);
		mounted = true;
		const observer = new MutationObserver(() => patch(document));
		observer.observe(document.body, { childList: true, subtree: true });
		return () => observer.disconnect();
	});
</script>

{#if mounted}
	<a class="skip-link" href="#main-content">Skip to content</a>
{/if}

<EvidenceDefaultLayout {data}>
	<slot slot="content" />
</EvidenceDefaultLayout>

<style>
	.skip-link {
		position: absolute;
		left: 0.5rem;
		top: -3rem;
		z-index: 100;
		padding: 0.5rem 1rem;
		background: #ffffff;
		color: #1e3a8a;
		border: 2px solid #1e3a8a;
		border-radius: 0.25rem;
	}
	.skip-link:focus {
		top: 0.5rem;
	}
	:global(#main-content:focus) {
		outline: none !important;
	}
	:global(:focus-visible) {
		outline: 2px solid #2563eb !important;
		outline-offset: 2px !important;
	}
	/* Target size (WCAG 2.5.8): breadcrumbs and the table search box. */
	:global(div.capitalize > a),
	:global(input) {
		min-height: 24px;
		display: inline-flex;
		align-items: center;
	}
	@media (prefers-reduced-motion: reduce) {
		:global(*),
		:global(*::before),
		:global(*::after) {
			animation: none !important;
			transition: none !important;
			scroll-behavior: auto !important;
		}
	}
</style>
