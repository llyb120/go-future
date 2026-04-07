export function hasGlobalDynamicsData({ input, asArray }) {
  const data = input || {};
  const hasMobile = asArray(data.mobile).some((module) => {
    return asArray(module?.top_list).length > 0 || asArray(module?.anomalies).length > 0;
  });
  const hasSteam = asArray(data.steam?.top_list).length > 0 || asArray(data.steam?.anomalies).length > 0;
  const hasRoblox = asArray(data.roblox?.top_list).length > 0 || asArray(data.roblox?.anomalies).length > 0;
  return hasMobile || hasSteam || hasRoblox;
}

export function buildGlobalDynamicsDailyReport({ input, asArray }) {
  const data = input || {};
  const normalized = {
    report_date: data.report_date || "",
    mobile: asArray(data.mobile).map((module) => ({
      country_region: module?.country_region || "",
      top_list: asArray(module?.top_list),
      anomalies: asArray(module?.anomalies),
    })),
    steam: {
      top_list: asArray(data.steam?.top_list),
      anomalies: asArray(data.steam?.anomalies),
    },
    roblox: {
      top_list: asArray(data.roblox?.top_list),
      anomalies: asArray(data.roblox?.anomalies),
    },
  };

  return {
    data: normalized,
    exist: hasGlobalDynamicsData({ input: normalized, asArray }),
  };
}
