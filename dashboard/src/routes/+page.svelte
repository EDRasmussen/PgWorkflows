<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import * as Accordion from '$lib/components/ui/accordion';
	import { Badge, type BadgeVariant } from '$lib/components/ui/badge';
	import { Button } from '$lib/components/ui/button';
	import * as Dialog from '$lib/components/ui/dialog';
	import { Input } from '$lib/components/ui/input';
	import * as Select from '$lib/components/ui/select';
	import * as Table from '$lib/components/ui/table';
	import ArrowDownIcon from '@lucide/svelte/icons/arrow-down';
	import ArrowUpIcon from '@lucide/svelte/icons/arrow-up';
	import MoonIcon from '@lucide/svelte/icons/moon';
	import RadioIcon from '@lucide/svelte/icons/radio';

	let { data } = $props();

	const STATUSES = ['pending', 'running', 'succeeded', 'failed'];

	const statusVariant = (status: string): BadgeVariant => {
		switch (status) {
			case 'failed':
				return 'destructive';
			case 'succeeded':
				return 'secondary';
			case 'running':
				return 'default';
			default:
				return 'outline';
		}
	};

	const fmt = (d: Date | null) => (d ? d.toLocaleString() : '–');

	const withParams = (mutate: (params: URLSearchParams) => void) => {
		const params = new URLSearchParams(page.url.searchParams);
		mutate(params);
		return `?${params}`;
	};

	const setFilter = (name: string, value: string | null) =>
		goto(
			withParams((params) => {
				params.delete('after');
				params.delete('before');
				if (value) params.set(name, value);
				else params.delete(name);
			}),
			{ keepFocus: true }
		);

	const pageHref = (kind: 'after' | 'before', cursor: string | null) =>
		withParams((params) => {
			params.delete('after');
			params.delete('before');
			if (cursor) params.set(kind, cursor);
		});

	const openRun = (id: string) =>
		goto(
			withParams((params) => params.set('run', id)),
			{ noScroll: true, keepFocus: true }
		);
	const closeRun = () =>
		goto(
			withParams((params) => params.delete('run')),
			{ noScroll: true, keepFocus: true }
		);

	const submitSearch = (event: SubmitEvent) => {
		event.preventDefault();
		const value = new FormData(event.currentTarget as HTMLFormElement).get('q');
		setFilter('q', value ? String(value).trim() || null : null);
	};
</script>

{#snippet activityItem(
	key: string,
	seq: number,
	name: string,
	status: string,
	input: unknown,
	result: unknown,
	error: string | null
)}
	<Accordion.Item value={key}>
		<Accordion.Trigger>
			<span class="flex items-center gap-2">
				<span class="text-muted-foreground">#{seq + 1}</span>
				{name}
				<Badge variant={statusVariant(status)}>{status}</Badge>
			</span>
		</Accordion.Trigger>
		<Accordion.Content class="space-y-3">
			{@render json('Input', input)}
			{#if error !== null}
				{@render json('Error', error)}
			{:else}
				{@render json('Output', result)}
			{/if}
		</Accordion.Content>
	</Accordion.Item>
{/snippet}

{#snippet json(label: string, value: unknown)}
	<div>
		<div class="mb-1 text-xs font-medium text-muted-foreground">{label}</div>
		<pre
			class="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs leading-relaxed">{JSON.stringify(
				value,
				null,
				2
			)}</pre>
	</div>
{/snippet}

<main class="container mx-auto py-16">
	<h1 class="scroll-m-20 text-4xl font-extrabold tracking-tight text-balance mb-6">PgWorkflows</h1>

	<section>
		<h2 class="mb-2 text-sm font-medium text-muted-foreground">Last 24 hours</h2>
		<div class="flex gap-4">
			{#each data.statusCounts as { status, count } (status)}
				<div class="rounded-lg border px-4 py-3">
					<div class="text-sm text-muted-foreground">{status}</div>
					<div class="text-2xl font-semibold">{count}</div>
				</div>
			{:else}
				<p class="text-muted-foreground">No runs in the last 24 hours.</p>
			{/each}
		</div>
	</section>

	<section class="mt-8 flex flex-wrap items-center gap-2">
		<form onsubmit={submitSearch}>
			<Input
				name="q"
				value={data.filters.q}
				placeholder="Search by run id…"
				class="w-80 font-mono text-xs"
			/>
		</form>

		<Select.Root
			type="single"
			value={data.filters.workflow ?? 'all'}
			onValueChange={(v: string | null) => setFilter('workflow', v === 'all' ? null : v)}
		>
			<Select.Trigger class="w-48">
				{data.filters.workflow ?? 'All workflows'}
			</Select.Trigger>
			<Select.Content>
				<Select.Item value="all">All workflows</Select.Item>
				{#each data.workflows as workflow (workflow)}
					<Select.Item value={workflow}>{workflow}</Select.Item>
				{/each}
			</Select.Content>
		</Select.Root>

		<Select.Root
			type="single"
			value={data.filters.status ?? 'all'}
			onValueChange={(v: string | null) => setFilter('status', v === 'all' ? null : v)}
		>
			<Select.Trigger class="w-40">
				{data.filters.status ?? 'All statuses'}
			</Select.Trigger>
			<Select.Content>
				<Select.Item value="all">All statuses</Select.Item>
				{#each STATUSES as status (status)}
					<Select.Item value={status}>{status}</Select.Item>
				{/each}
			</Select.Content>
		</Select.Root>
	</section>

	<section class="mt-4">
		<Table.Root>
			<Table.Header>
				<Table.Row>
					<Table.Head>Workflow</Table.Head>
					<Table.Head>Status</Table.Head>
					<Table.Head>Steps</Table.Head>
					<Table.Head>
						<button
							class="flex items-center gap-1"
							onclick={() => setFilter('dir', data.filters.dir === 'desc' ? 'asc' : 'desc')}
						>
							Created
							{#if data.filters.dir === 'desc'}
								<ArrowDownIcon class="size-3.5" />
							{:else}
								<ArrowUpIcon class="size-3.5" />
							{/if}
						</button>
					</Table.Head>
					<Table.Head>Completed</Table.Head>
				</Table.Row>
			</Table.Header>
			<Table.Body>
				{#each data.runs as run (run.workflow_run_id)}
					<Table.Row class="cursor-pointer" onclick={() => openRun(run.workflow_run_id)}>
						<Table.Cell class="font-medium">{run.workflow_name}</Table.Cell>
						<Table.Cell>
							<div class="flex items-center gap-1.5">
								<Badge variant={statusVariant(run.status)}>{run.status}</Badge>
								{#if run.sleeping_until && !run.completed_at}
									<Badge variant="outline">
										<MoonIcon />
										sleeping until {fmt(run.sleeping_until)}
									</Badge>
								{/if}
								{#if run.waiting_for_signal && !run.completed_at}
									<Badge variant="outline">
										<RadioIcon />
										waiting for "{run.waiting_for_signal}"
									</Badge>
								{/if}
							</div>
						</Table.Cell>
						<Table.Cell>
							{#if run.steps_total > 0}
								{run.steps_done} / {run.steps_total}
							{:else}
								–
							{/if}
						</Table.Cell>
						<Table.Cell>{fmt(run.created_at)}</Table.Cell>
						<Table.Cell>{fmt(run.completed_at)}</Table.Cell>
					</Table.Row>
				{:else}
					<Table.Row>
						<Table.Cell colspan={5} class="text-center text-muted-foreground">
							No matching workflow runs.
						</Table.Cell>
					</Table.Row>
				{/each}
			</Table.Body>
		</Table.Root>

		<div class="mt-4 flex items-center justify-end gap-2">
			<Button
				variant="outline"
				size="sm"
				disabled={!data.page.hasPrev || data.runs.length === 0}
				href={data.page.hasPrev && data.runs.length > 0
					? pageHref('before', data.page.prevCursor)
					: undefined}
			>
				Previous
			</Button>
			<Button
				variant="outline"
				size="sm"
				disabled={!data.page.hasNext || data.runs.length === 0}
				href={data.page.hasNext && data.runs.length > 0
					? pageHref('after', data.page.nextCursor)
					: undefined}
			>
				Next
			</Button>
		</div>
	</section>
</main>

<Dialog.Root open={data.selected !== null} onOpenChange={(open: boolean) => !open && closeRun()}>
	<Dialog.Content class="sm:max-w-2xl">
		{#if data.selected}
			{@const { run, steps, failureHooks, signalWaits, timers } = data.selected}
			<Dialog.Header>
				<Dialog.Title class="flex items-center gap-2">
					{run.workflow_name}
					<Badge variant={statusVariant(run.status)}>{run.status}</Badge>
				</Dialog.Title>
				<Dialog.Description class="font-mono text-xs">
					{run.workflow_run_id}
				</Dialog.Description>
			</Dialog.Header>

			<div class="max-h-[60vh] space-y-4 overflow-y-auto pr-1">
				<div class="space-y-1 text-sm text-muted-foreground">
					<p>Started {fmt(run.created_at)} · attempt {run.attempt} / {run.max_attempts}</p>
					{#each timers as timer (timer.fire_at)}
						<p class="flex items-center gap-1.5">
							<MoonIcon class="size-3.5" />
							Sleeping until {fmt(timer.fire_at)}
						</p>
					{/each}
					{#each signalWaits as wait (wait.signal_name)}
						<p class="flex items-center gap-1.5">
							<RadioIcon class="size-3.5" />
							Waiting for signal "{wait.signal_name}" since {fmt(wait.created_at)}
						</p>
					{/each}
				</div>

				{#if run.input !== null}
					{@render json('Workflow input', run.input)}
				{/if}

				<Accordion.Root type="multiple">
					{#each steps as step (step.step_seq)}
						{@render activityItem(
							`step-${step.step_seq}`,
							step.step_seq,
							step.activity_name,
							step.status,
							step.input,
							step.result,
							step.error
						)}
					{:else}
						<p class="text-sm text-muted-foreground">No steps recorded yet.</p>
					{/each}
				</Accordion.Root>

				{#if failureHooks.length > 0}
					<div>
						<h3 class="text-sm font-medium">Compensations</h3>
						<Accordion.Root type="multiple">
							{#each failureHooks as hook (hook.hook_seq)}
								{@render activityItem(
									`hook-${hook.hook_seq}`,
									hook.hook_seq,
									hook.activity_name,
									hook.status,
									hook.input,
									hook.result,
									hook.error
								)}
							{/each}
						</Accordion.Root>
					</div>
				{/if}

				{#if run.error !== null}
					{@render json('Workflow error', run.error)}
				{:else if run.result !== null}
					{@render json('Workflow result', run.result)}
				{/if}
			</div>
		{/if}
	</Dialog.Content>
</Dialog.Root>
