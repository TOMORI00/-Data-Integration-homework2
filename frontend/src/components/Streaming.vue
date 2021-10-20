<template>
  <div>
    <robot :src-data="zkRobot" title="撞库机器人" label="机器人ID"/>
    <robot :src-data="qdRobot" title="抢单机器人" label="机器人ID"/>
    <robot :src-data="sdRobot" title="刷单机器人" label="机器人ID"/>
    <robot :src-data="pcRobot" title="爬虫机器人" label="机器人IP"/>
  </div>
</template>

<script>
import * as streaming from "@/data/Streaming.json"
import Robot from "@/components/Robot";

export default {
  name: "Streaming",
  components:{
    robot:Robot
  },
  data() {
    return {
      zkRobot: streaming.Robot1,
      qdRobot: streaming.Robot2,
      sdRobot: streaming.Robot3,
      pcRobot: streaming.Robot4
    }
  },
}
</script>

<style scoped>

</style>