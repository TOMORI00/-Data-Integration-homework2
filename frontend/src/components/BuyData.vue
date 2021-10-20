<template>
  <div>
    <div style="margin-top:-10px;margin-bottom: -10px">
      <!--      <el-button type="text" @click="handleAngleChange('user')"><h3>用户角度</h3></el-button>-->
      <!--      <el-divider direction="vertical"/>-->
      <el-button type="text" @click="handleAngleChange('category')"><h3>商品类别角度</h3></el-button>
      <el-divider direction="vertical"/>
      <el-button type="text" @click="handleAngleChange('item')"><h3>商品角度</h3></el-button>
    </div>
    <!--    <User v-show="mode.user"></User>-->
    <Category v-show="mode.category"></Category>
    <Item v-show="mode.item"></Item>
  </div>
</template>

<script>
import User from "@/components/User";
import Category from "@/components/Category";
import Item from "@/components/Item";

export default {
  name: "BuyData",
  components: {
    User,
    Category,
    Item
  },
  data() {
    return {
      mode: {
        user: false,
        category: true,
        item: false,
      }
    }
  },
  methods: {
    handleAngleChange(mode) {
      const _this = this
      for (let key in _this.mode) {
        _this.mode[key] = (key === mode);
      }
    }
  }
}
</script>

<style scoped>
h3 {
  font-weight: 15;
  font-family: "Microsoft YaHei UI Light";
  font-size: 16px;
}
</style>