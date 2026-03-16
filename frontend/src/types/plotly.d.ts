// Type declarations for plotly.js-dist-min
// The package ships without its own TypeScript types; this module
// declaration provides the minimum surface used by the explorer.

declare module 'plotly.js-dist-min' {
  /** Data trace passed to Plotly. */
  type Data = Record<string, unknown>;

  /** Layout configuration object. */
  type Layout = Record<string, unknown>;

  /** Config object controlling Plotly behaviour. */
  type Config = Record<string, unknown>;

  /** A DOM element extended with Plotly internals. */
  type PlotlyHTMLElement = HTMLElement & { data?: Data[]; layout?: Layout };

  /**
   * Create a new Plotly chart inside `root`.
   * Any existing chart in `root` is replaced.
   */
  function newPlot(
    root: string | PlotlyHTMLElement,
    data: Data[],
    layout?: Partial<Layout>,
    config?: Partial<Config>,
  ): Promise<PlotlyHTMLElement>;

  /**
   * React-style update — efficiently updates an existing chart,
   * creating it if it does not yet exist.
   */
  function react(
    root: string | PlotlyHTMLElement,
    data: Data[],
    layout?: Partial<Layout>,
    config?: Partial<Config>,
  ): Promise<PlotlyHTMLElement>;

  /**
   * Remove the chart and all associated event listeners from `root`,
   * releasing memory held by the underlying WebGL context.
   */
  function purge(root: string | PlotlyHTMLElement): void;

  /**
   * Update the layout of an existing chart without re-drawing data traces.
   * Accepts a partial layout object; only the specified keys are updated.
   */
  function relayout(
    root: string | PlotlyHTMLElement,
    update: Partial<Layout>,
  ): Promise<PlotlyHTMLElement>;
}
