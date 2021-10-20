<template>
  <div>
    <el-divider content-position="left"><h2>商品成交率：成交次数 / 浏览次数</h2></el-divider>
    <div class="knowledge-div">
      <el-table
          max-height="290px"
          :data="transactionRate"
          :header-cell-style="{'text-align':'center'}"
          :cell-style="{'text-align':'center'}"
          stripe
          border>
        <el-table-column type="index" :index="indexMethod"/>
        <el-table-column
            prop="itemID"
            label="商品ID">
        </el-table-column>
        <el-table-column
            prop="rate"
            label="商品成交率">
        </el-table-column>
      </el-table>
    </div>
    <el-divider content-position="left"><h2>热门商品：高访问量，高成交量</h2></el-divider>
    <div class="knowledge-div">
      <el-table
          max-height="290px"
          :data="popularItem"
          :header-cell-style="{'text-align':'center'}"
          :cell-style="{'text-align':'center'}"
          stripe
          border>
        <el-table-column type="index" :index="indexMethod"/>
        <el-table-column
            prop="itemID"
            label="热门商品ID">
        </el-table-column>
        <el-table-column
            prop="count"
            label="评价数值">
        </el-table-column>
      </el-table>
    </div>
    <el-divider content-position="left"><h2>商品复购率：复购次数 / 购买次数</h2></el-divider>
    <div class="knowledge-div">
    <el-table
          max-height="290px"
          :data="repurchaseRate"
          :header-cell-style="{'text-align':'center'}"
          :cell-style="{'text-align':'center'}"
          stripe
          border>
        <el-table-column type="index" :index="indexMethod"/>
        <el-table-column
            prop="itemID"
            label="商品ID">
        </el-table-column>
        <el-table-column
            prop="rate"
            label="复购率">
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script>
import * as CombinationData from "@/data/Combination.json"

export default {
  name: "Combination",
  data() {
    return {
      transactionRate: CombinationData.transactionRate,
      repurchaseRate: CombinationData.repurchaseRate,
      popularItem: CombinationData.popularItem
    }
  },
  methods: {
    indexMethod(index) {
      return index + 1;
    },
  },
}
</script>

<style scoped>

</style>