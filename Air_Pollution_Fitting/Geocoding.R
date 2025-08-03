#本脚本用于地理编码，即将具体地址转化为经纬度，基于百度API，客户端ak运行
#加载如下包，若未安装请通过install.packages('')进行安装
library(httr)
library(jsonlite)
library(dplyr)

#读取待拟合的表格，请确保表格为csv文件，置于'./Geocoding/put_your_data_here_GC/'文件夹中
#请将你的地址列列名命名为addr
df <- read.csv(list.files('./Geocoding/put_your_data_here_GC/', pattern = "\\.csv$", full.names = T)[1])

# 1. 定义多个AK密钥
#请在此输出你的百度api客户端ak密钥，支持多个ak与多日循环进行
aks <- c('')


# 2. 初始化地理编码函数（支持AK参数传递）
baidu_geocoding <- function(address_input, ak) {
  tryCatch({
    # 对地址进行URL编码
    encoded_address <- URLencode(address_input, reserved = TRUE)
    
    # 构建请求URL
    address_url <- paste0('https://api.map.baidu.com/geocoding/v3/?address=', 
                          encoded_address, '&output=json&ak=', ak)
    
    # 发送请求并处理响应
    response <- GET(address_url)
    json_content <- content(response, as = "text", encoding = "UTF-8")
    geo <- fromJSON(json_content)
    
    # 检查返回状态
    if (geo$status != 0) {
      return(list(status = geo$status, message = geo$message))
    }
    
    # 提取数据
    result <- list(
      lng = geo$result$location$lng,
      lat = geo$result$location$lat,
      confidence = ifelse(!is.null(geo$result$confidence), geo$result$confidence, NA),
      comprehension = ifelse(!is.null(geo$result$comprehension), geo$result$comprehension, NA),
      level = ifelse(!is.null(geo$result$level), geo$result$level, NA),
      status = 0
    )
    return(result)
    
  }, error = function(e) {
    return(list(status = -1, message = e$message))
  })
}

# 3. 主处理函数
batch_geocode_with_ak_rotation <- function(df) {
  # 添加存储结果的列
  if (!"geocoded" %in% names(df)) df$geocoded <- FALSE
  if (!"lng" %in% names(df)) df$lng <- NA_real_
  if (!"lat" %in% names(df)) df$lat <- NA_real_
  if (!"confidence" %in% names(df)) df$confidence <- NA_real_
  if (!"comprehension" %in% names(df)) df$comprehension <- NA_integer_
  if (!"level" %in% names(df)) df$level <- NA_character_
  
  # 创建AK状态追踪器
  ak_status <- data.frame(
    ak = aks,
    daily_limit_reached = FALSE,
    stringsAsFactors = FALSE
  )
  
  # 主循环
  while (any(!df$geocoded)) {
    current_ak <- NULL
    
    # 查找可用AK
    for (i in 1:nrow(ak_status)) {
      if (!ak_status$daily_limit_reached[i]) {
        current_ak <- ak_status$ak[i]
        cat("使用AK:", substr(current_ak, 1, 6), "***\n")
        break
      }
    }
    
    # 所有AK都达到限额时的处理
    if (is.null(current_ak)) {
      # 计算到次日2点的等待时间
      next_midnight <- as.POSIXct(paste(Sys.Date() + 1, "2:00:00"))
      wait_seconds <- as.numeric(difftime(next_midnight, Sys.time(), units = "secs"))
      
      if (wait_seconds > 0) {
        cat("所有AK均达到每日限额，等待至次日2点（", 
            format(next_midnight, "%Y-%m-%d %H:%M:%S"), "）...\n")
        Sys.sleep(wait_seconds)
      }
      
      # 重置所有AK状态
      ak_status$daily_limit_reached <- FALSE
      current_ak <- ak_status$ak[1]
      cat("重置所有AK状态，重新开始编码\n")
    }
    
    # 处理未编码的行
    for (i in which(!df$geocoded)) {
      result <- baidu_geocoding(df$addr[i], current_ak)
      
      # 成功获取结果
      if (!is.null(result$status) && result$status == 0) {
        df$lng[i] <- result$lng
        df$lat[i] <- result$lat
        df$confidence[i] <- result$confidence
        df$comprehension[i] <- result$comprehension
        df$level[i] <- result$level
        df$geocoded[i] <- TRUE
        
        # 显示进度
        completed <- sum(df$geocoded)
        total <- nrow(df)
        cat("✅ [", completed, "/", total, "] 成功编码行", i, "-", 
            substr(df$addr[i], 1, 20), ifelse(nchar(df$addr[i]) > 20, "...", ""), 
            "\n", sep = "")
      } 
      # 处理限额错误（包括状态码2和302）
      else if (!is.null(result$status) && (result$status %in% c(2, 302))) {
        cat("🛑 AK", substr(current_ak, 1, 6), "*** 达到每日限额（状态码", 
            result$status, ": ", result$message, "）\n")
        ak_status$daily_limit_reached[ak_status$ak == current_ak] <- TRUE
        break  # 跳出当前循环，更换AK
      }
      # 其他错误处理
      else if (!is.null(result$status) && result$status != 0) {
        cat("⚠️ 编码错误（行", i, "): 状态码", result$status, "-", 
            result$message, "\n")
        Sys.sleep(1)  # 错误时短暂等待
      }
      # 网络错误等特殊情况
      else {
        cat("⚠️ 未知错误（行", i, "), 等待后重试...\n")
        Sys.sleep(5)  # 长时间等待后重试
      }
      
      # 每次请求后短暂暂停（遵守API速率限制）
      Sys.sleep(0.5)
      
      # 每处理100个地址保存进度（防止中断丢失数据）
      if (sum(df$geocoded) %% 100 == 0) {
        saveRDS(df, "./Geocoding/geocoding_progress.RDS")
        cat("⏫ 保存处理进度（已完成", sum(df$geocoded), "条地址）\n")
      }
    }
  }
  
  # 完成后保存最终结果
  saveRDS(df, "./Geocoding/geocoding_complete.RDS")
  cat("🎉 所有地址地理编码完成！总计", nrow(df), "条地址\n")
  
  return(df)
}

# 4. 执行地理编码
# 假设您的数据框名为df，包含addr列，请将上述函数加载进入环境后直接运行以下语句即可
df <- batch_geocode_with_ak_rotation(df)
#运行完成后，结果会储存在"./Geocoding/geocoding_complete.RDS"文件中
#这是一个持续数天的地理编码R程序，请不要关闭这个R窗口谢谢


# 5. 恢复进度（如果中断）
# 如果您需要从上次中断处恢复，使用：
# df <- readRDS("geocoding_progress.RDS")
# df <- batch_geocode_with_ak_rotation(df)

#6. 特殊情况
# 若出现一些特殊字符开头的地址或空地址，可能导致程序无法终止，此时直接终止R程序即可
# 使用上述中断处文件作为结果