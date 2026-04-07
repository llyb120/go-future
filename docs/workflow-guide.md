# 工作流编制说明

这份文档是 `go-ai-future` 当前工作流系统的完整编制说明。

目标有两个：

1. 让编写者知道**每个标签、每个属性、每个执行阶段**到底做什么。
2. 让工作流尽量保持**可读、可写、可维护**，把复杂度收敛到少量可组合的工具标签里。

---

## 1. 设计目标

当前工作流系统的核心原则是：

- SQL 尽量保持简单
- 数据依赖关系显式写在 workflow 里
- 复杂结构组装交给 workflow
- 特别复杂的处理再交给 JS
- 所有跨边界值尽量保持 JSON 兼容

也就是说：

- `sql` 负责“查什么”
- `var / pick / transform` 负责“怎么把结果继续变成下一步需要的形态”
- `transform mode="js"` 负责“声明式能力不够时的复杂加工”

---

## 2. 一个完整 workflow 的执行模型

一个工作流从上到下顺序执行。

最重要的执行规则如下：

1. 先读取 `input`
2. 再按 XML 中出现的顺序执行每个顶级步骤
3. 每个步骤产出的结果都会写入变量上下文
4. 后续步骤可以直接引用前面步骤产出的变量
5. `sql` 可以有多个，并且后面的 SQL 可以依赖前面的 SQL 结果
6. 最终所有中间变量和结果都会出现在执行上下文里

所以 workflow 的编排，本质上就是：

```text
input -> var/pick/transform -> sql -> var/pick/transform -> sql -> ... -> final result
```

---

## 3. workflow 的基本骨架

```xml
<workflow name="example" title="示例工作流" description="说明">
  <input ... />

  <var ... />
  <pick ... />
  <transform ... />
  <sql ... />
</workflow>
```

### 3.1 `workflow` 顶层属性

- `name`
  - 必填
  - 工作流唯一名称
  - 用于页面选择和内部标识

- `title`
  - 选填
  - 展示名称
  - 不填时默认使用 `name`

- `description`
  - 选填
  - 页面说明文案

---

## 4. 输入：`input`

`input` 只负责声明工作流入口参数。

### 4.1 写法

```xml
<input name="customerEmail" label="客户邮箱" default="alice.future@demo.ai" />
```

或者：

```xml
<input name="payload" type="json"><![CDATA[
{
  "tenant": "acme"
}
]]></input>
```

### 4.2 支持的属性

- `name`
  - 必填
  - 输入变量名

- `label`
  - 选填
  - 页面展示名

- `description`
  - 选填
  - 页面说明

- `placeholder`
  - 选填
  - 表单占位提示

- `default`
  - 选填
  - 默认值

- `type`
  - 选填
  - 当前支持：
    - `text`
    - `number`
    - `textarea`
    - `json`

- `required`
  - 选填
  - `true / false`
  - 为 `true` 且输入为空时执行报错

### 4.3 `json` 类型说明

当 `type="json"` 时：

- 输入值会先按 JSON 解析
- 解析结果会变成 `map[string]any`、`[]any`、`string`、`float64`、`bool`、`nil` 等 JSON 兼容值
- 后续可以用 `pick` 或 `var from="..."` 按选择器继续取值

---

## 5. 变量：`var`

`var` 用于定义或者覆盖一个变量。

它是最常用的工具标签。

### 5.1 最推荐的写法

简单场景直接一行：

```xml
<var name="customerEmailKey" from="customerEmail" default="alice.future@demo.ai" op="trim,lower" />
```

这表示：

1. 从 `customerEmail` 取值
2. 如果为空就用默认值
3. 再做 `trim` 和 `lower`
4. 把结果写入 `customerEmailKey`

### 5.2 `var` 的过程式写法

如果你想显式串联步骤，也可以写成：

```xml
<var name="tenantCode">
  <pick from="payload > tenant" />
  <transform op="trim,upper" />
</var>
```

但是当前推荐优先使用一行式写法，只有在逻辑真的需要拆开时再写子步骤。

### 5.3 `var` 支持的属性

- `name`
  - 必填
  - 结果变量名

- `from`
  - 选填
  - 数据来源
  - 会按选择器语法解析

- `value`
  - 选填
  - 固定字面值

- `template`
  - 选填
  - 模板字符串，例如 `%{{keyword}}%`

- `default`
  - 选填
  - 当前值为空时使用的默认值

- `op`
  - 选填
  - 对值执行一串转换操作

- `optional`
  - 选填
  - 当 `from` 找不到时，不立即报错，而是返回空值，供 `default` 或后续逻辑处理

### 5.4 `var` 的求值顺序

`var` 的执行顺序是：

1. 先根据 `from / value / template / 子步骤` 取得原始值
2. 如果值为空且定义了 `default`，则应用默认值
3. 如果定义了 `op`，按顺序执行转换
4. 最终写入 `name`

---

## 6. 取值：`pick`

`pick` 用于从复杂结构里选数据。

一般来说，当前更推荐直接把选择器写在 `var from="..."` 里；但如果你想让“取值”语义显式可见，也可以单独使用 `pick`。

### 6.1 写法

```xml
<pick name="paidOrders" from="orders[status=paid]" />
```

或嵌套在 `var` 中：

```xml
<var name="customerId">
  <pick from="customerRows > :first > id" />
</var>
```

### 6.2 `pick` 支持的属性

- `name`
  - 顶级 `pick` 必填
  - 嵌套在 `var` 里时可省略

- `from`
  - 选填
  - 来源对象
  - 不写时：
    - 在 `var` 内部默认取当前值
    - 顶级步骤默认从根变量上下文开始

- `path`
  - 选填
  - 对来源对象继续做选择器筛选

- `default`
  - 选填
  - 选择结果为空时的默认值

- `optional`
  - 选填
  - 找不到时不报错

- `op`
  - 选填
  - 对取到的值继续做转换

---

## 7. 选择器语法（新风格）

当前统一使用类 DOM/CSS 风格选择器。

### 7.1 key 选择器

直接写 key 名：

```text
customer
orders
product
```

### 7.2 层级连接

推荐使用：

- 空格
- `>`

语义分别是：

- `>`：只匹配直属下一层
- 空格：匹配任意后代层级

例如：

```text
payload > filters > keyword
customerRows > :first > id
catalog items product name
```

注意：

- 同一层级不要把空格和 `>` 混成一种语义
- `catalog > items` 与 `catalog items` 的结果可能完全不同
- 当前不再推荐旧的点号分隔写法

### 7.3 属性选择器

支持：

- `[status=paid]`
- `[status!=pending]`
- `[items]`

示例：

```text
orders[status=paid]
orders[status!=cancelled]
orders[items]
```

### 7.4 伪类

当前支持：

- `:first`
- `:last`
- `:eq(n)`
- `:keys`

示例：

```text
orders > :first
orders > :eq(1)
productById:keys
```

### 7.5 `:keys` 的含义

`:keys` 是工作流扩展伪类。

作用：

- 当当前值是 `map` 时，返回这个 map 的全部 key
- 返回结果为字符串数组
- 当前实现会按字典序排序

示例：

```text
productById:keys
```

### 7.6 选择器在数组上的行为

当选择器作用于数组时：

- key 选择器会对数组里的每个元素分别尝试取值
- 成功的结果会被收集成新数组

例如：

```text
orders > id
```

表示：

- 先拿到 `orders` 数组
- 再从每个 order 里取 `id`
- 最终得到 `id` 列表

### 7.7 当前推荐写法

推荐：

```text
customerRows > :first > id
orders > id
productById > {{product_id}}
```

---

## 8. 结构变换：`transform`

`transform` 是工作流里最强的结构处理工具。

当前支持以下模式：

- `object`
- `tree`
- `map`
- `group`
- `index`
- `js`

如果 `transform` 带有 `<field>` 子节点但没写 `mode`，默认按 `object` 处理。

### 8.1 `object`：声明式改结构

```xml
<transform name="normalizedRequest">
  <field path="tenant" from="tenantCode" />
  <field path="filters.keyword" from="keyword" />
  <field path="filters.statuses" from="statusList" />
</transform>
```

作用：

- 创建一个新的对象
- 每个 `field` 决定往哪个路径写什么值

### 8.2 `field` 的属性

- `path`
  - 目标路径，例如 `filters.keyword`

- `from`
  - 数据来源

- `value`
  - 固定值

- `template`
  - 模板值

- `default`
  - 默认值

- `optional`
  - 找不到时不报错

- `op`
  - 字段级后处理

### 8.3 `tree`：平铺节点组树

```xml
<transform name="categoryTree" from="rows" mode="tree" id="id" parent="parent_id" children="children" root="0" />
```

含义：

- `from`：平铺数组
- `id`：节点唯一标识字段
- `parent`：父节点字段
- `children`：子节点数组字段名
- `root`：根节点父值

### 8.4 `map`：逐条改结构

```xml
<transform name="ordersView" from="orders" mode="map">
  <field path="id" from="id" />
  <field path="orderNo" from="order_no" />
  <field path="items" from="itemsByOrder > {{id}}" optional="true" default="[]" op="json" />
</transform>
```

作用：

- 输入是数组
- 对每个元素执行一次 `object` 式映射
- 输出还是数组

### 8.5 `group`：按字段分组

```xml
<transform name="itemsByOrder" from="itemsWithProduct" mode="group" by="orderId" />
```

作用：

- 输入数组
- 按 `by` 指定字段分组
- 输出 `map[string][]any`

### 8.6 `index`：按字段建索引

```xml
<transform name="productById" from="products" mode="index" by="id" />
```

作用：

- 输入数组
- 按 `by` 指定字段建立索引
- 输出 `map[string]any`

### 8.7 `js`：复杂逻辑交给 JS

当声明式模式不够时，使用：

```xml
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
```

注意：

- workflow 里只写处理代码
- 不需要自己写 `host.export(...)`
- 系统会自动包装成可执行函数

### 8.8 JS 外部源码引用

如果不想把 JS 直接写进 XML，可以使用 `src`：

```xml
<transform name="customerOrderStats" mode="js" from="customerOrderView" src="scripts\customer-order-stats.js" entry="customerOrderStats" />
```

规则：

- `src` 路径默认相对于当前 workflow XML 文件
- 支持直接引用：
  - `.js`
  - `.mjs`
  - `.cjs`
  - `.md`
- 如果使用 Markdown：
  - 系统会查找 `js` 或 `javascript` fenced code block
  - `src="xxx.md#heading-name"` 时，会优先在对应标题下找代码块
- `src` 与内嵌 `<![CDATA[...]]>` 二选一，不能同时写
- 如果一个文件导出多个函数，需要通过 `entry` 指定入口函数

外部 JS 支持两种写法：

1. 文件里只写一个处理体，系统自动包装成默认入口
2. 文件里写多个 `export function` / `export async function`，再由 `entry` 选择本次调用哪个

示例：

```js
export function collectItems({ input, asArray }) {
  return asArray(input?.orders).flatMap((order) => asArray(order?.items));
}

export async function customerOrderStats({ input, keys, asArray }) {
  const view = input || { customer: {}, orders: [] };
  const orders = asArray(view.orders);

  return {
    totalOrders: orders.length,
    totalItems: collectItems({ input: view, asArray }).length,
    rootKeys: await keys(view),
  };
}
```

### 8.9 JS 中可用的变量和辅助函数

当前 JS transform 默认注入：

- `input`
  - 当前 transform 的输入值

- `vars`
  - 当前整个工作流变量上下文

- `current`
  - 当前值（通常与 `input` 一致，用于兼容过程式场景）

- `pick(value, selector)`
  - 异步 helper
  - 复用 workflow 的选择器

- `keys(value)`
  - `pick(value, ":keys")` 的快捷方式

- `asArray(value)`
  - 方便把单值或空值规整成数组

### 8.10 JS transform 的约束

- 当前实现参考 `go-v8-unified-demo`
- Windows 下通过 **Node.js 子进程** 执行
- Go 和 JS 之间只传 **JSON 兼容类型**
- 不建议依赖 JS 里的宿主环境特性
- 输入和输出都应是 JSON 兼容值

---

## 9. 操作链：`op`

当前支持的操作：

- `trim`
- `upper`
- `lower`
- `string`
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

### 9.1 常见示例

```xml
<var name="tenantCode" from="tenant" op="trim,upper" />
<var name="limitNum" from="limit" default="10" op="int" />
<var name="statusList" from="orders > status" optional="true" default="[]" op="json" />
<var name="sortColumn" from="payload > page > sort > column" default="id" op="trim,lower,allow(id|name|email|status)" />
```

### 9.2 `default` 与 `op` 的顺序

顺序固定为：

1. 先取值
2. 为空时应用 `default`
3. 再执行 `op`

---

## 10. SQL：`sql`

`sql` 负责执行数据库查询或更新。

### 10.1 常用属性

- `name`
  - 选填
  - 如果填写，SQL 结果会被写入变量上下文
  - `query` 时一般写成结果数组
  - `exec` 时一般写成 `{ rowsAffected, lastInsertID }`

- `mode`
  - `query`
  - `exec`

- `engine`
  - 不写时默认普通 SQL
  - `gosql` 表示动态 SQL 模板

- `datasource`
  - 当前默认是 `default`

### 10.2 外部 SQL 引用

如果 SQL 很长，或者你想把 SQL 和 workflow 拆开维护，可以给 `sql` 配 `src`：

```xml
<sql name="orders" mode="query" engine="gosql" src="snippets\customer-orders.md#orders-by-customer" />
```

当前支持：

- `.md`
- `.markdown`

规则：

- `src` 路径默认相对于当前 workflow XML 文件
- `src` 与内嵌 SQL 二选一，不能同时写
- 外部 SQL 只从 Markdown 读取，这样与 `gosql` 的官方组织方式一致
- 如果引用 Markdown：
  - 只读取 `sql` fenced code block
  - 是否按 `gosql` 语法执行，由当前 `<sql>` 的 `engine` 属性决定
  - `src="xxx.md#heading-name"` 时，会优先在对应标题下找代码块

Markdown 片段示例：

````md
## orders-by-customer

```sql
select id, customer_id, order_no, status, created_at
from orders
where customer_id = @customerId
@if orderStatus != "" {
  and status = @orderStatus
}
order by id
```
````

### 10.3 多 SQL 的依赖链

当前系统允许在一个 workflow 里写多个 SQL。

推荐写法：

1. 先用一个 SQL 查主对象
2. 从主对象结果里提取关键 ID
3. 再查下一层
4. 再提取下一层所需的 ID 列表
5. 继续往后查
6. 最后统一组装

这就是当前 `customer-orders-structured` 的组织方式。

### 10.4 SQL 结果变量的形态

`query` 结果写入变量时，形态是：

```go
[]map[string]any
```

所以：

- 第一行：`rows > :first`
- 所有 id：`rows > id`
- 某行的字段：`rows > :first > id`

### 10.5 普通 SQL 与 `gosql`

普通 SQL 适合：

- 固定语句
- 命名参数

`gosql` 适合：

- 条件拼接
- 列表参数
- 依赖前序变量的动态条件

---

## 11. 复杂示例：多 SQL 依赖 + workflow 组数 + JS summary

推荐直接看：

```text
workflows/customer-orders-structured.xml
workflows/customer-orders-external.xml
```

这个示例完整演示了：

- 多条简单 SQL
- 前序查询结果驱动后续查询
- `index / map / group / object` 组数
- `transform mode="js"` 做复杂统计
- `:keys` 伪类在 JS 里复用
- 把 SQL 和 JS 拆到 XML 外部

它的依赖链是：

```text
customerEmail
  -> customerRows
  -> customerId
  -> orders
  -> orderIds
  -> orderItems
  -> productIds
  -> products
  -> productById
  -> itemsWithProduct
  -> itemsByOrder
  -> ordersView
  -> customerOrderView
  -> customerOrderStats
```

---

## 12. 错误处理规则

### 12.1 引用找不到

- 默认直接报错
- 如果定义了 `optional="true"`，则返回空值
- 可以再配合 `default` 把空值转成安全值

### 12.2 选择器找不到

- 默认报错
- `optional="true"` 时不报错

### 12.3 JS 执行失败

- 会直接把脚本错误返回到 workflow 执行结果
- Windows 下 Node stderr 会被收集进错误信息

### 12.4 JSON 兼容值约束

跨 Go / JS 边界请只使用：

- `null`
- `bool`
- `number`
- `string`
- `[]any`
- `map[string]any`

---

## 13. 编写建议

推荐顺序：

1. 先定义 `input`
2. 先把关键主键变量拉出来
3. SQL 只查当前层需要的数据
4. 用 `var` 抽出后续查询所需 id 列表
5. 用 `index / group / map / object` 逐层组装
6. 最后真的不够，再上 `transform mode="js"`

推荐心智模型：

- **查数据**：`sql`
- **抽关键值**：`var / pick`
- **声明式组数**：`transform`
- **复杂逻辑补位**：`transform mode="js"`

---

## 14. 运行与验证

本地运行：

```bash
go run .
```

测试：

```bash
go test ./...
```

如果默认端口被占用：

```bash
ADDR=:18080 go run .
```

---

## 15. 当前示例工作流

- `sql-users-search.xml`
  - 简单变量整理 + 普通 SQL

- `sql-users-search-advanced.xml`
  - JSON 输入 + 声明式结构重组 + `gosql`

- `category-tree.xml`
  - 平铺数据直接组 tree

- `customer-orders-structured.xml`
  - 多 SQL 依赖链 + workflow 组数 + JS summary

- `customer-orders-external.xml`
  - 与上面同一个主题，但把 SQL 放到 Markdown，把 JS 放到独立脚本

---

## 16. 当前已实现能力边界

当前已经适合：

- 结构化数据抽取
- 多阶段查询编排
- 依赖式 ID 传递
- 多表结果组装
- 树结构生成
- 复杂 summary / reshaping 的 JS 补位

如果后续还要继续扩展，最值得加的方向是：

- 更强的属性选择器表达式
- 更多伪类
- 更强的数组聚合/透视 transform
- 更丰富的 JS helper
