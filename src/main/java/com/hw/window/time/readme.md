https://www.youtube.com/watch?v=YKsml00PvnM&list=PLmOn9nNkQxJGgsR8xuYpSwkkx293BHtr5&index=47

除了上游算子（是否选择keyBy）、window窗口类型（timewindow（滑动、滚动、
session会话）、countwindow（滑动、滚动））、下游
窗口计算函数（增量（reduce、aggregate）、全量（apply、process））之外
还有一下可选的API：如allowedLateness，不过这个算子是只有事件事件的时候才会生效，并且
可以将迟到的数据输出到sideOutputLateData，不过输出的要指定一个tag，后续可以使用getoutput算子
来获取这一部分迟到的数据。不过这些算子都依赖于指定事件时间，处理时间并不生效，默认窗口就是处理
时间，而不是事件时间