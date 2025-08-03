#本脚本用于制作暴露-时间-地点表格，以fst文件存储对应表格文件
#单个fst文件储存某省某年暴露-时间-地点数据，时间格式为YYYYMMDD(character)，地点对应各省点阵表格

# 请在使用该脚本制作前先于CHAP dataset官网下载对应数据集并符合以下规范
# ①将日分辨率数据置于'./rawdata/CHAP/'路径下，不同污染物置于不同文件夹如示例所示
# ②若想制作某年表格，请下载对应年份所有天数的数据并放置，缺失天数会导致拟合错误
# ③污染物文件夹的命名请使用CHAP官方命名，切勿更改（可直接复制粘贴文件名中的名称）

# 加载新增包
library(terra)
library(data.table)
library(future.apply) 

# 选择污染物模型、年份以及地区
pollutants <- c('PM2.5','O3') #可选多个污染物
years <- 2022:2023  #可选多年，以向量形式包含你的年份
provinces <- c('fj','gd') #可选多个地区，请使用简称，简称参考外部表格

# 主处理流程（优化版）
for (province in provinces) {
  # 提前加载网格点并转换格式（全省份共享）
  points_data <- read.fst(paste0('./rawdata/points_fst/', province, '_1km_grid.fst')) %>% 
    setNames(c("grid_point_name", "longitude", "latitude"))
  points_vect <- vect(points_data, geom = c("longitude", "latitude"), 
                      crs = "+proj=longlat +datum=WGS84")
  
  # 并行处理各污染物
  future_lapply(pollutants, function(ap) {
    # 处理各年份
    for (year in years) {
      nc_dir <- file.path('./rawdata/CHAP', ap, year)
      nc_files <- list.files(nc_dir, pattern = "\\.nc$", full.names = TRUE)
      
      # 预分配结果矩阵（日期数×网格点数）
      value_matrix <- matrix(NA_real_, nrow = length(nc_files), 
                             ncol = nrow(points_data))
      date_vec <- character(length(nc_files))
      
      # 批量处理每日文件
      for (i in seq_along(nc_files)) {
        # 高效读取NC数据
        rast <- rast(nc_files[i], subds = ap)
        crs(rast) <- "+proj=longlat +datum=WGS84"
        
        # 直接提取网格点值
        values <- terra::extract(rast, points_vect, ID = FALSE, method="bilinear")[[1]] %>% #此处使用双线性插值法
          signif(digits = 4)
        
        # 填充预分配矩阵
        value_matrix[i, ] <- values
        date_vec[i] <- strsplit(basename(nc_files[i]), "_")[[1]][4]
        message(paste(ap, date_vec[i], province, "processed"))
      }
      
      # 高效构建结果表
      result_dt <- data.table(date = date_vec, value_matrix)
      setnames(result_dt, c("date", points_data$grid_point_name))
      
      # na_cols <- names(which(sapply(result_dt, \(x) anyNA(x))))
      # result_dt <- result_dt[, !na_cols, with = FALSE]
      
      # 写入磁盘
      write_fst(result_dt, paste0('./AP_Time_tables/', ap, '_', year, '_', province, '.fst'))
    }
  }, future.seed = TRUE)
}
