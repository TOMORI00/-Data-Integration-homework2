<template>
  <div>
    <el-divider content-position="left"><h2>{{ title }}</h2></el-divider>
    <div class="knowledge-div">
      <el-table
          max-height="290px"
          :data="data"
          :header-cell-style="{'text-align':'center'}"
          :cell-style="{'text-align':'center'}"
          stripe
          border>
        <el-table-column type="index" :index="indexMethod" width="60px"/>
        <el-table-column
            :prop="prop"
            :label="label">
        </el-table-column>
      </el-table>
      <div style="margin-top: 5px">
        <el-pagination
            layout="prev, pager, next"
            :total="tableData.length / 50 * 10"
            @current-change="handlePageChange">
        </el-pagination>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: "Robot",
  data() {
    return {
      tableData: [],
      data: [],
      pageIndex: 1,
      prop: "",
    }
  },
  props: {
    title: String,
    srcData: Array,
    label: String,
  },
  methods: {
    handlePageChange(index) {
      this.data = this.tableData.slice(50 * index - 50, 50 * index)
      this.pageIndex = index
    },
    indexMethod(index) {
      return this.pageIndex * 50 + index - 49;
    },
  },
  mounted() {
    if (this.label === "机器人ID") {
      this.prop = "userID"
    } else if (this.label === "机器人IP") {
      this.prop = "ipAddr"
    }
    this.tableData = this.srcData
    this.handlePageChange(1)
  }
}
</script>

<style scoped>

</style>