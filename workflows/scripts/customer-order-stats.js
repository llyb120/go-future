export function collectItems({ input, asArray }) {
  const orders = asArray(input?.orders);
  return orders.flatMap((order) => asArray(order?.items));
}

export async function customerOrderStats({ input, keys, asArray }) {
  const view = input || { customer: {}, orders: [] };
  const orders = asArray(view.orders);
  const items = collectItems({ input: view, asArray });

  return {
    customerName: view.customer?.name || "",
    totalOrders: orders.length,
    totalItems: items.reduce((sum, item) => sum + (item.quantity || 0), 0),
    totalAmount: items.reduce((sum, item) => sum + (item.amount || 0), 0),
    rootKeys: await keys(view),
  };
}
