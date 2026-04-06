# go-ai-future

一个用 Go 编写的工作流型 Web 项目原型。

这个项目的重点不是继续堆业务 API，而是把“工具”和“工作流”变成一等能力。当前版本已经支持用 **XML 定义工作流**，并把几个内置工具标签串起来执行：

- `input`：声明输入
- `var`：定义或覆盖变量，简单场景可以一行写完
- `pick`：从复杂结构里取值
- `transform`：把任意结构变成另一种结构
- `sql`：执行 SQL，支持普通参数化 SQL 和 `gosql` 动态 SQL

详细编制说明见：`docs/workflow-guide.md`

## 当前能力

- 基于 Go `net/http` 的 Web 页面
- XML 工作流定义，层级尽量少
- 支持复杂 JSON 输入
- 支持路径选择器
  - `payload > filters > keyword`
  - `payload > filters > statuses > :eq(0)`
  - `payload > filters > statuses[*]`
- 支持结构变换
  - `trim`
  - `upper`
  - `lower`
  - `int`
  - `float`
  - `bool`
  - `json`
  - `json_string`
  - `split(...)`
  - `join(...)`
  - `first`
  - `last`
  - `flat`
  - `allow(...)`
- `sql` 支持两种模式
  - 普通 SQL：命名参数 `:tenantCode`
  - `gosql`：动态条件、循环、表达式和安全参数化
- 默认使用 SQLite，开箱可跑

## XML 设计

目标是“人能直接写”，所以把能力收敛成少量工具标签。

### 简单示例

```xml
<workflow name="user-search" title="租户用户查询" description="使用内置工具标签做变量整理，然后执行 SQL 查询用户。">
  <input name="tenant" label="租户编码" required="true" />
  <input name="keyword" label="关键字" default="" />
  <input name="limit" label="返回条数" default="10" type="number" />

  <var name="tenantCode" from="tenant" op="trim,upper" />
  <var name="keyword" from="keyword" default="" op="trim" />
  <var name="keywordLike" template="%{{keyword}}%" />
  <var name="limitNum" from="limit" default="10" op="trim,int" />

  <sql mode="query" datasource="default"><![CDATA[
SELECT id, tenant, name, email, status
FROM users
WHERE tenant = :tenantCode
  AND (:keyword = '' OR name LIKE :keywordLike OR email LIKE :keywordLike)
ORDER BY id
LIMIT :limitNum;
  ]]></sql>
</workflow>
```

### 复杂结构 + 动态 SQL 示例

```xml
<workflow name="user-search-advanced" title="复杂结构查询">
  <input name="payload" type="json"><![CDATA[
{
  "tenant": "acme",
  "filters": {
    "keyword": "alice",
    "statuses": ["active"]
  },
  "page": {
    "limit": 10,
    "offset": 0,
    "sort": {
      "column": "id",
      "direction": "desc"
    }
  }
}
  ]]></input>

  <var name="tenantCode" from="payload > tenant" op="trim,upper" />
  <var name="keyword" from="payload > filters > keyword" optional="true" default="" op="trim" />
  <var name="keywordLike" template="%{{keyword}}%" />
  <var name="statusList" from="payload > filters > statuses[*]" optional="true" default="[]" op="json" />
  <var name="limitNum" from="payload > page > limit" optional="true" default="10" op="int" />
  <var name="offsetNum" from="payload > page > offset" optional="true" default="0" op="int" />
  <var name="sortColumn" from="payload > page > sort > column" optional="true" default="id" op="trim,lower,allow(id|name|email|status)" />
  <var name="sortDirection" from="payload > page > sort > direction" optional="true" default="desc" op="trim,lower,allow(asc|desc)" />

  <transform name="normalizedRequest">
    <field path="tenant" from="tenantCode" />
    <field path="filters.keyword" from="keyword" />
    <field path="filters.statuses" from="statusList" />
    <field path="page.limit" from="limitNum" />
    <field path="page.offset" from="offsetNum" />
    <field path="page.sort.column" from="sortColumn" />
    <field path="page.sort.direction" from="sortDirection" />
  </transform>

  <sql mode="query" engine="gosql" datasource="default"><![CDATA[
select id, tenant, name, email, status
from users
where tenant = @tenantCode
@if len(statusList) > 0 {
  and status in (@statusList)
}
order by @=sortColumn @=sortDirection
limit @limitNum
offset @offsetNum
  ]]></sql>
</workflow>
```

这套格式里：

- `var` 适合简单变量规整，常见场景可直接用 `from/default/op`
- `pick` 更适合从复杂对象/数组中抽取信息
- `transform` 更适合把已有值重组成新结构，尤其适合 `object` 和 `tree`
- `sql` 只负责真正的数据库执行

## `pick` 选择器约定

当前统一使用更接近 DOM/CSS 的选择器写法：

- `payload > tenant`
- `payload > filters > statuses[*]`
- `orders[status=paid] > items > :first > product`
- `orders > :eq(1) > status`
- `productById:keys`

当前支持的选择器子集：

- key 选择器：`orders`、`product`
- 属性选择器：`[status=paid]`、`[status!=pending]`、`[items]`
- 伪类：`:first`、`:last`、`:eq(n)`、`:keys`
 - 连接方式：推荐使用空格和 `>`

其中 `:keys` 是扩展伪类，用来获取 map 的全部 key。

## `transform` 的推荐用法

### 1. 改结构：声明式 object

如果只是把已有变量改成另一种结构，推荐直接写 `field`，比 JSON 模板更短：

```xml
<transform name="normalizedRequest">
  <field path="tenant" from="tenantCode" />
  <field path="filters.keyword" from="keyword" />
  <field path="filters.statuses" from="statusList" />
  <field path="page.limit" from="limitNum" />
</transform>
```

### 2. 组 tree：内置 tree 模式

如果输入是一组平铺节点，可以直接组树：

```xml
<sql name="rows" mode="query"><![CDATA[
SELECT id, parent_id, name
FROM categories
ORDER BY id;
]]></sql>

<transform name="categoryTree" from="rows" mode="tree" id="id" parent="parent_id" children="children" root="0" />
```

这类场景不再需要自己写模板、循环或手工拼 children。

### 3. 多表关联后返回带结构的结果

下面这个示例会分别查询 `customers`、`orders`、`order_items`、`products`，最后由工作流把它们组装成嵌套结构：

```xml
<workflow name="customer-orders-structured" title="客户订单结构视图">
  <input name="customerEmail" default="alice.future@demo.ai" />
  <input name="orderStatus" default="" />

  <var name="customerEmailKey" from="customerEmail" default="alice.future@demo.ai" op="trim,lower" />
  <var name="orderStatus" from="orderStatus" default="" op="trim,lower" />

  <sql name="customerRows" mode="query"><![CDATA[
SELECT id, name, email, level
FROM customers
WHERE lower(email) = :customerEmailKey;
  ]]></sql>

  <var name="customerId" from="customerRows > :first > id" op="int" />

  <sql name="orders" mode="query" engine="gosql"><![CDATA[
select id, customer_id, order_no, status, created_at
from orders
where customer_id = @customerId
@if orderStatus != "" {
  and status = @orderStatus
}
order by id
  ]]></sql>

  <var name="orderIds" from="orders > id" optional="true" default="[]" op="json" />

  <sql name="orderItems" mode="query" engine="gosql"><![CDATA[
SELECT id, order_id, product_id, quantity, unit_price, quantity * unit_price AS amount
FROM order_items
where
@if len(orderIds) > 0 {
  order_id in (@orderIds)
} else {
  1 = 0
}
ORDER BY id;
  ]]></sql>

  <var name="productIds" from="orderItems > product_id" optional="true" default="[]" op="json" />

  <sql name="products" mode="query" engine="gosql"><![CDATA[
SELECT id, name, price
FROM products
where
@if len(productIds) > 0 {
  id in (@productIds)
} else {
  1 = 0
}
ORDER BY id;
  ]]></sql>

  <transform name="productById" from="products" mode="index" by="id" />
  <transform name="itemsWithProduct" from="orderItems" mode="map">
    <field path="id" from="id" />
    <field path="orderId" from="order_id" />
    <field path="quantity" from="quantity" />
    <field path="amount" from="amount" />
    <field path="product" from="productById > {{product_id}}" />
  </transform>

  <transform name="itemsByOrder" from="itemsWithProduct" mode="group" by="orderId" />
  <transform name="ordersView" from="orders" mode="map">
    <field path="id" from="id" />
    <field path="orderNo" from="order_no" />
    <field path="items" from="itemsByOrder > {{id}}" optional="true" default="[]" op="json" />
  </transform>

  <transform name="customerOrderView">
    <field path="customer" from="customerRows > :first" />
    <field path="orders" from="ordersView" />
  </transform>

  <transform name="customerOrderStats" mode="js" from="customerOrderView"><![CDATA[
const view = input || { customer: {}, orders: [] };
const orders = Array.isArray(view.orders) ? view.orders : [];
const items = orders.flatMap((order) => Array.isArray(order.items) ? order.items : []);

return {
  customerName: view.customer?.name || "",
  totalOrders: orders.length,
  totalItems: items.reduce((sum, item) => sum + (item.quantity || 0), 0),
  totalAmount: items.reduce((sum, item) => sum + (item.amount || 0), 0),
  rootKeys: await keys(view),
};
  ]]></transform>
</workflow>
```

这里的思路是：

- 第一条 SQL 先拿到 customer，再从 `customerRows > :first > id` 提取 `customerId`
- 第二条 SQL 用 `customerId` 去查 orders，再从 `orders > id` 提取 `orderIds`
- 第三条 SQL 用 `orderIds` 去查 orderItems，再从 `orderItems > product_id` 提取 `productIds`
- 第四条 SQL 再用 `productIds` 去查 products
- `index` 负责把产品列表做成查表字典
- `map` 负责逐条改结构
- `group` 负责按外键把子项归并到父级
- 最后再用一个 `object` 风格的 `transform` 拼成最终返回结果
- 如果结构处理特别复杂，再用 `transform mode="js"` 做补充

### 4. 特别复杂的处理：交给 JS

当 `object / tree / map / group / index` 还不够时，可以直接用 `transform mode="js"`：

```xml
<transform name="customerOrderStats" mode="js" from="customerOrderView"><![CDATA[
const view = input;
const orders = Array.isArray(view.orders) ? view.orders : [];
const items = orders.flatMap((order) => order.items || []);

return {
  totalOrders: orders.length,
  totalAmount: items.reduce((sum, item) => sum + (item.amount || 0), 0),
  rootKeys: await keys(view),
};
]]></transform>
```

当前实现参考了你给的 `go-v8-unified-demo` 思路：

- Windows 下运行时启动 Node.js 子进程
- Go 和 JS 之间只传 JSON 兼容值
- Workflow 里只写处理代码，不需要自己写 `host.export(...)`
- Go 会自动把代码包进 `run` 函数，并注入 `input / vars / current`
- JS 内可以直接用 `await pick(value, selector)` 和 `await keys(value)` 复用 workflow 的选择器

## `gosql` 说明

项目已经集成 `https://github.com/llyb120/gosql`，因此 `sql` 标签可以直接写动态 SQL 模板。

推荐：

- 用 `@var` 做参数化
- 用 `@if` 控制条件块
- 用 `@for` 做循环
- 用 `@=` 只处理白名单内的表名、列名、排序方向

当前示例里通过 `allow(...)` 先把排序字段和方向规整后，再交给 `@=` 输出，避免把未校验输入直接拼进 SQL。

## 本地运行

```bash
go run .
```

启动后打开：

```text
http://localhost:8080
```

页面里现在有四个示例工作流：

- `user-search`：简单变量整理 + 普通 SQL
- `user-search-advanced`：复杂 JSON 输入 + 简化后的声明式结构重组 + `gosql`
- `category-tree`：先查平铺分类，再直接组 tree
- `customer-orders-structured`：多表关联后返回带结构的客户订单视图

## 测试

```bash
go test ./...
```

## 目录结构

```text
.
├─ internal
│  ├─ data
│  ├─ jsruntime
│  ├─ web
│  └─ workflow
├─ workflows
│  ├─ sql-users-search.xml
│  ├─ sql-users-search-advanced.xml
│  ├─ category-tree.xml
│  └─ customer-orders-structured.xml
├─ data
│  └─ demo.db
├─ main.go
└─ README.md
```

## 下一步适合扩展的方向

- 把 `pick` 选择器继续扩展成更完整的复杂结构查询语法
- 给 `transform` 增加更多声明式结构映射能力，例如数组映射、分组聚合、按字段透视
- 一个工作流里支持多个 `sql` 或更多内置工具
- 加入可配置数据源、权限、审计和执行历史
