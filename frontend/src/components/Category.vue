<template>
  <div>
    <el-divider content-position="left"><h2>最受欢迎：购买次数最多的商品类别</h2></el-divider>
    <div class="knowledge-div">
      <el-table
          max-height="290px"
          :data="mostPopularCategory"
          :header-cell-style="{'text-align':'center'}"
          :cell-style="{'text-align':'center'}"
          stripe
          border>
        <el-table-column type="index" :index="indexMethod"/>
        <el-table-column
            prop="categoryID"
            label="商品类别ID">
        </el-table-column>
        <el-table-column
            prop="count"
            label="购买总数">
        </el-table-column>
      </el-table>
    </div>
    <el-divider content-position="left"><h2>最近热销：短时间内购买次数最多的商品类别</h2></el-divider>
    <div class="knowledge-div">
      <el-table
          max-height="290px"
          :data="recentPopularCategory"
          :header-cell-style="{'text-align':'center'}"
          :cell-style="{'text-align':'center'}"
          stripe
          border>
        <el-table-column type="index" :index="indexMethod"/>
        <el-table-column
            prop="categoryID"
            label="商品类别ID">
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
import Category from '@/data/Category.json'

export default {
  name: "Category",
  data() {
    return {
      mostPopularCategory: Category.mostPopular,
      recentPopularCategory: Category.recentPopular
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