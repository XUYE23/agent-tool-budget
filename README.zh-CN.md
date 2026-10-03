# Agent Tool Budget

面向单次 Agent 运行的异步工具预算管理，核心零第三方依赖。

安装：python -m pip install .
演示：agent-tool-budget demo --output outputs/demo.json
测试：python -m unittest discover -s tests -v

主要功能：调用次数、估算成本与总时限控制；重复只读请求合并；
有限重试；运行内 TTL 缓存；不含参数与结果正文的事件记录。
每次实际尝试提前扣除预算，失败与重试均计入成本。

工具默认为写操作语义，每个请求执行一次。只读或幂等声明需由调用方保证。
预算单位由调用方定义，实际 token 计费需连接供应商用量接口。
取消采用 Python 异步协作机制，远端已执行的操作需要业务层处理。

演示使用合成异步工具，可复现重复读取减少与超预算阻止行为。
框架接入边界和限制见英文 README、docs/semantics.md。
