# #package_install请安装你尚未安装的包，若已经安装可跳过
# install.packages('tidyverse')
# install.packages('lubridate')
# install.packages('geosphere')
# install.packages('data.table')
# install.packages('fst')


#暴露拟合核心程序基于以下exposure_lag_fitting函数，请先将该函数加载到环境中
#（将光标置于函数第一行，回车加载）
#加载完成后在最下方调用函数，设置里所需要的参数
#若仅需使用，不需要理解以下函数，有问题请联系icesophi@163.com或lentu本人微信


exposure_lag_fitting <- function(basen, ti, provinces,cores = detectCores()-4) {
  # 加载必要的包
  require(tidyverse)
  require(data.table)
  require(lubridate)
  require(geosphere)
  require(fst)
  require(stringr)
  require(parallel)
  
  # 自定义辅助函数定义
  flexible_date_converter <- function(x) {
    orig_na <- sum(is.na(x))
    if (inherits(x, "Date")) return(x)
    
    if (is.numeric(x)) {
      if (all(x > 19000000, na.rm = TRUE)) {
        conv <- as.Date(as.character(x), format = "%Y%m%d")
      } else {
        conv <- as.Date(x, origin = "1899-12-30")
      }
    } else if (is.character(x)) {
      formats <- c("ymd", "ymd HMS", "ymd HM", "ymd H", "mdy", "dmy")
      dates <- parse_date_time(x, orders = formats, quiet = TRUE)
      conv <- as.Date(dates)
    } else {
      conv <- rep(NA_Date_, length(x))
    }
    
    # 检测转换错误并警告
    new_na <- sum(is.na(conv)) - orig_na
    if (new_na > 0) {
      message(sprintf("警告：日期转换失败 %d 条记录，NA值增加 %d", 
                      sum(is.na(conv)), new_na))
    }
    return(conv)
  }
  
  get_year_range <- function(dates, days, data_source) {
    if (!inherits(dates, "Date")) stop("输入必须为Date类型向量", call. = FALSE)
    if (length(dates) == 0) {
      warning("输入日期向量为空，返回NA", call. = FALSE)
      return(c(NA_character_, NA_character_))
    }
    max_date <- max(dates, na.rm = TRUE)
    min_date <- min(dates, na.rm = TRUE)
    adjusted_date <- min_date - days
    year_max <- as.numeric(format(max_date, "%Y"))
    year_adjusted <- as.numeric(format(adjusted_date, "%Y"))
    c(year_adjusted, year_max)
  }#该函数用于计算年份范围，便于提取数据库
  
  merge_pollution_data <- function(file_paths) {
    file_info <- tibble(file_path = file_paths) %>%
      mutate(
        file_name = basename(file_path),
        year = str_extract(file_name, "(?<=_)\\d{4}(?=_)") %>% as.integer(),
        province = str_extract(file_name, "(?<=_)[a-z]+(?=\\.fst$)"),
        sort_index = dense_rank(year) * 1000 + dense_rank(province)
      ) %>%
      arrange(sort_index)
    
    year_groups <- split(file_info, file_info$year)
    
    yearly_data <- map(year_groups, function(group) {
      base_df <- read_fst(group$file_path[1], as.data.table = TRUE)
      if (nrow(group) > 1) {
        province_data <- map(group$file_path[-1], function(path) {
          fst_columns <- metadata_fst(path)$columnNames
          read_fst(path, columns = fst_columns[-1], as.data.table = TRUE)
        })
        bind_cols(base_df, !!!province_data)
      } else {
        base_df
      }
    })
    
    bind_rows(yearly_data) %>% arrange(date)
  }#该函数用于将所提取的数据库进行融合使用
  
  # 主函数逻辑
  message('正在读取待拟合数据')
  casetofit <- fread(list.files('./put_your_data_here/', pattern = "\\.csv$", full.names = T)[1])
  
  # 转换日期并加强检测
  casetofit$date <- flexible_date_converter(casetofit$date)
  valid_dates <- !is.na(casetofit$date)
  if (!all(valid_dates)) {
    message(sprintf("警告：发现 %d 条无效日期记录，已过滤", sum(!valid_dates)))
    casetofit <- casetofit[valid_dates, ]
  }
  
  year_range <- get_year_range(casetofit$date, ti, casetofit)
  
  # 读取网格点数据
  message('正在读取网格数据')
  file_paths_points <- file.path("./rawdata/points_fst/", paste0(provinces, "_1km_grid.fst"))
  pointdata <- rbindlist(
    lapply(file_paths_points, read_fst),
    use.names = TRUE, 
    fill = FALSE,
    idcol = "origin_province"
  )
  
  # 读取空气污染数据
  message('正在处理网格数据')
  file_list_ap <- list.files('./AP_Time_tables/', pattern = "\\.fst$")
  file_list_ap <- str_subset(file_list_ap, str_c(provinces, collapse = "|"))
  file_list_ap <- str_subset(file_list_ap, str_c(year_range[1]:year_range[2], collapse = "|"))
  
  # 处理污染数据
  merge_list <- list()
  for(province in provinces) {
    file_list_ap_sub <- str_subset(file_list_ap, province)
    merged_data <- bind_rows(lapply(paste0('./AP_Time_tables/', file_list_ap_sub), 
                                    function(path) read_fst(path, to = 5)))
    merge_list[[length(merge_list) + 1]] <- merged_data[, 2:ncol(merged_data)]
  }
  
  merged_df <- do.call(cbind, merge_list) %>% select(where(~ !any(is.na(.x))))
  pointdata <- pointdata %>% filter(point_id %in% colnames(merged_df))
  
  # 点-案例匹配
  message('正在计算最近网格')
  casetofit_coords <- as.matrix(casetofit[, .(lon, lat)])
  pointdata_coords <- as.matrix(pointdata[, .(lon, lat)])
  
  # 设置并行计算
  cl <- makeCluster(cores)
  clusterExport(cl, c("pointdata", "distHaversine"))
  clusterEvalQ(cl, library(geosphere))
  
  # 并行计算最近点
  nearest_points <- parLapply(cl, 1:nrow(casetofit_coords), function(i) {
    distances <- distHaversine(casetofit_coords[i, ], pointdata_coords)
    pointdata$point_id[which.min(distances)]
  }) %>% unlist()
  
  stopCluster(cl)
  
  casetofit$nearest_points <- nearest_points
  basetofit <- casetofit %>% select(nearest_points, date) %>%
    mutate(date = format(date, '%Y%m%d'))
  unique_points <- unique(nearest_points)
  
  # 暴露滞后计算
  message('正在计算暴露滞后')
  # 3. 暴露滞后计算的错误处理
  expofit <- function(ap) {
    col_name <- paste(ap, 'lag', (ti-1):0, sep = '_')
    
    database <- tryCatch({
      ap_file_list <- str_subset(paste0('./AP_Time_tables/', file_list_ap), ap)
      merge_pollution_data(ap_file_list) %>% 
        select(all_of(c('date', unique_points))) %>%
        mutate(date = as.Date(date, format = "%Y%m%d"))
    }, error = function(e) {
      message("污染数据加载失败: ", conditionMessage(e))
      return(NULL)
    })
    
    # 数据库检查
    if (is.null(database) || nrow(database) == 0) {
      message(ap, " 数据库不可用，返回空值")
      return(matrix(NA, nrow = nrow(basetofit), ncol = ti,
                    dimnames = list(NULL, col_name)))
    }
    
    outcome_list <- vector("list", nrow(basetofit))
    
    for (i in 1:nrow(basetofit)) {
      point_id <- basetofit[[i, 1]]
      if (!point_id %in% colnames(database)) {
        outcome_list[[i]] <- rep(NA, ti)
        next
      }
      
      # 添加错误处理
      outcome_list[[i]] <- tryCatch({
        id_date <- as.Date(basetofit[[i, 2]], format = "%Y%m%d")
        target_dates <- seq(id_date - (ti - 1), id_date, by = "day")
        valid_rows <- match(target_dates, database$date)
        
        if (any(is.na(valid_rows))) {
          rep(NA, ti)
        } else {
          database[valid_rows, ][[point_id]]
        }
      }, error = function(e) {
        message(sprintf("行 %d 计算失败: %s", i, conditionMessage(e)))
        rep(NA, ti)
      })
    }
    
    df_outcome <- setNames(data.frame(do.call(rbind, outcome_list)), col_name)
    message(paste(ap, 'finished'))
    return(df_outcome)
  }
  
  # 执行主计算
  aps <- unique(sapply(strsplit(file_list_ap, "_", fixed = TRUE), function(x) x[1]))
  result_list <- lapply(aps, expofit)
  
  message('正在汇总拟合结果')
  combined_outcome <- bind_cols(casetofit, result_list)
  
  # 保存结果
  if (!dir.exists("./outcome/")) dir.create("./outcome/")
  message('正在将结果储存至outcome文件夹')
  write_fst(combined_outcome, paste0('./outcome/', basen, '.fst'))
  
  return(combined_outcome)
}


# 请将你需要拟合的对象置于put_your_data_here文件夹中，确保其为csv格式，且包含以下列
# 1、经度，且命名为lon
# 2、纬度，且命名为lat
# 3、参考时间，且命名为date

result <- exposure_lag_fitting(
  basen = "testdata", #请在此输入拟合例子的不带后缀的名称，并将文件置于casetofit文件夹
  ti = 20, #请在此输入你需要的时间窗口长度,请注意，该长度包括当天（例：填2，则为当天加上前一天）
  provinces = c("fj", "gd"),#请在此处输入你的省份简称，支持跨、多省份，但请注意多省份时运行可能较慢
  cores = 4
)

#完成后，结果会以fst格式储存至outcome文件夹，也可以点击环境中的result进行检查
