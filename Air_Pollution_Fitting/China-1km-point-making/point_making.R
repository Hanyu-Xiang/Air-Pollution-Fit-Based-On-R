library(sf)
library(dplyr)
library(purrr)
library(tidyr)

# 读取省级SHP文件（示例路径）
province_sf <- st_read("./China-1km-point-making/省.shp") %>% 
  st_transform(4326)  # 转为WGS84坐标系

# 为每个省份生成1km×1km网格点
generate_grid_points <- function(geom) {
  bbox <- st_bbox(geom)  # 获取边界框
  grid <- st_make_grid(
    st_as_sfc(bbox),
    cellsize = c(0.00899, 0.00899),  # 1km在赤道≈0.00899度
    what = "centers"                  # 取格网中心点
  )
  st_intersection(grid, geom)  # 仅保留省内的点
}

# 并行处理各省网格
grid_points <- province_sf %>%
  group_by(省) %>%
  group_modify(~ st_sf(geometry = generate_grid_points(.x$geometry)))

province_sf <- province_sf %>% filter(省 != '中朝共有')

province_vec <- province_sf$省

prov_abbr <- c(
  "bj",  # 北京市 (Bei Jing) [7](@ref)
  "tj",  # 天津市 (Tian Jin) [7](@ref)
  "heb",  # 河北省 (He Bei) → (@ref)
  "sx",  # 山西省 (Shan Xi) [7](@ref)
  "nm",  # 内蒙古自治区 (Nei Meng) [7](@ref)
  "ln",  # 辽宁省 (Liao Ning) [7](@ref)
  "jl",  # 吉林省 (Ji Lin) [7](@ref)
  "hl",  # 黑龙江省 (Hei Long) [7](@ref)
  "sh",  # 上海市 (Shang Hai) [7](@ref)
  "js",  # 江苏省 (Jiang Su) [7](@ref)
  "zj",  # 浙江省 (Zhe Jiang) [7](@ref)
  "ah",  # 安徽省 (An Hui) [7](@ref)
  "fj",  # 福建省 (Fu Jian) [7](@ref)
  "jx",  # 江西省 (Jiang Xi) [7](@ref)
  "sd",  # 山东省 (Shan Dong) [7](@ref)
  "hen",  # 河南省 (He Nan) → 与湖南"hn"冲突，取"豫"的拼音首字母退位[7,10](@ref)
  "hb",  # 湖北省 (Hu Bei) [7](@ref)
  "hun", # 湖南省 (Hu Nan) → 与河南/海南冲突，扩展为三字母[7,10](@ref)
  "gd",  # 广东省 (Guang Dong) [7](@ref)
  "gx",  # 广西壮族自治区 (Guang Xi) [7](@ref)
  "hin",  # 海南省 (Hai Nan) → 与河南"ha"区分，取"海"+"南"末字母[7,10](@ref)
  "cq",  # 重庆市 (Chong Qing) [7](@ref)
  "sc",  # 四川省 (Si Chuan) [7](@ref)
  "gz",  # 贵州省 (Gui Zhou) [7](@ref)
  "yn",  # 云南省 (Yun Nan) [7](@ref)
  "xz",  # 西藏自治区 (Xi Zang) [7](@ref)
  "sn",  # 陕西省 (Shan Xi) → 与山西"sx"区分，取"陕"+"西"末字母[7,10](@ref)
  "gs",  # 甘肃省 (Gan Su) [7](@ref)
  "qh",  # 青海省 (Qing Hai) [7](@ref)
  "nx",  # 宁夏回族自治区 (Ning Xia) [7](@ref)
  "xj",  # 新疆维吾尔自治区 (Xin Jiang) [7](@ref)
  "tw",  # 台湾省 (Tai Wan) [7](@ref)
  "xg",  # 香港特别行政区 (Xiang Gang) [7](@ref)
  "am"   # 澳门特别行政区 (Ao Men) [7](@ref)
)

province_sf$abbr <- prov_abbr

ref_table <- data.frame(province = province_sf$省,abbr = province_sf$abbr)
writexl::write_xlsx(ref_table,'./省份简称参考表.xlsx')

for (ab in prov_abbr){
  df1 <- ref_table %>% filter(abbr == ab)
  pv <- df1[1,1]
  result <- grid_points %>%
    filter(省 == pv) %>% 
    mutate(
      point_id = paste0( # 生成点名称（如BJ_1）
        ab, "_",
        row_number()
      ),
      lon = st_coordinates(geometry)[, 1],  # 提取经度
      lat = st_coordinates(geometry)[, 2]    # 提取纬度
    ) %>%
    as_tibble() %>%
    select(point_id, lon, lat)
  
  # 保存为fst
  fst::write.fst(result, paste0('./China-1km-point-making/points_fst/',ab,"_1km_grid.fst"))
}
