interface SegmentAnalytics {
  page: (name?: string) => void;
  reset: () => void;
  alias: (newId: string) => void;
  track: (name: string, properties: any) => void;
  identify: (traits: any, userId?: string) => void;
  group: (organisationId: string, traits: any) => void;
}

export class AnalyticsService {
  private static getAnalytics = (): SegmentAnalytics | undefined =>
    (window as any).analytics;

  static alias = (newId: string) =>
    AnalyticsService.getAnalytics()?.alias?.(newId);

  static page = (name: string) => AnalyticsService.getAnalytics()?.page?.(name);

  static reset = () => AnalyticsService.getAnalytics()?.reset?.();

  static track = (name: string, properties: any) =>
    AnalyticsService.getAnalytics()?.track?.(name, properties);

  static identify = (traits: any = {}, userId?: string) =>
    AnalyticsService.getAnalytics()?.identify?.(traits, userId);

  static group = (organisationId: string, traits: any = {}) =>
    AnalyticsService.getAnalytics()?.group?.(organisationId, traits);
}
