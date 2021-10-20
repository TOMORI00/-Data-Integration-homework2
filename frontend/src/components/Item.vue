<template>
  <div>
    <el-divider content-position="left"><h2>最受欢迎：购买次数最多的商品</h2></el-divider>
    <div class="knowledge-div">
      <el-table
          max-height="290px"
          :data="mostPopularItem"
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
            prop="count"
            label="购买总数">
        </el-table-column>
      </el-table>
    </div>
    <el-divider content-position="left"><h2>最近热销：短时间（一周）内购买次数最多的商品</h2></el-divider>
    <div class="knowledge-div">
      <el-table-column type="index" :index="indexMethod"/>
      <el-table
          max-height="290px"
          :data="recentPopularItem"
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
            prop="count"
            label="购买总数">
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script>
import itemData from '@/data/Item.json'

export default {
  name: "Item",
  data() {
    return {
      mostPopularItem: itemData.mostPopular,
      recentPopularItem: itemData.recentPopular
    }
  },
  methods: {
    indexMethod(index) {
      return index + 1;
    }
  }
}
</script>

<style scoped>
.knowledge-div {
  margin: 30px 40px
}
</style>