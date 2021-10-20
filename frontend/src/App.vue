<template>
  <div id="app">
    <!--    <img alt="Vue logo" src="./assets/logo.png">-->
    <!--    <HelloWorld msg="Welcome to Your Vue.js App"/>-->
    <el-container>
      <el-header height="62px">
        <h1 style="margin-top: 16px;margin-bottom: 16px;font-size: 30px;">G7-Homework2</h1>
      </el-header>
      <el-main>
        <el-tabs type="border-card" id="main-tab">
          <el-tab-pane label="交易数据">
            <buy-data></buy-data>
          </el-tab-pane>
          <el-tab-pane label="流数据">
            <Streaming></Streaming>
          </el-tab-pane>
          <el-tab-pane label="组合数据">
            <Combination></Combination>
          </el-tab-pane>
        </el-tabs>
      </el-main>
    </el-container>
  </div>
</template>

<script>
import HelloWorld from './components/HelloWorld.vue'
import BuyData from "@/components/BuyData";
import Streaming from "@/components/Streaming";
import Combination from "@/components/Combination";

export default {
  name: 'App',
  components: {
    HelloWorld,
    "buy-data":BuyData,
    Streaming,
    Combination
  }
}
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  /*margin-top: 60px;*/
}

body {
  margin: 0px;
}

.el-header{
  border-bottom: 1px solid gainsboro;
}

.el-main{
  padding: 20px;
}

#main-tab{
  /*height: 850px;*/
  border-radius: 8px;
}

h2{
  font-weight: normal;
  font-size: 20px;
  font-family:"楷体", "STXingkai";
}

.knowledge-div {
  margin: 30px 40px
}
</style>
