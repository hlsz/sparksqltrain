package com.data.utils;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


class ExcelOutPut {
    private ArrayList<String> titleList;

    private ArrayList<ArrayList<String>> dataList;

    public ExcelOutPut() {
    }

    public ExcelOutPut(ArrayList<String> titleList, ArrayList<ArrayList<String>> dataList) {
        this.titleList = titleList;
        this.dataList = dataList;
    }

    public ArrayList<String> getTitleList() {
        return titleList;
    }

    public void setTitleList(ArrayList<String> titleList) {
        this.titleList = titleList;
    }

    public ArrayList<ArrayList<String>> getDataList() {
        return dataList;
    }

    public void setDataList(ArrayList<ArrayList<String>> dataList) {
        this.dataList = dataList;
    }
}




public class PoiHelper {

    /**
     * 读取xlsx文件
     *
     * @param io 文件绝对路径
     * @throws Exception
     */
    public static ExcelOutPut readXlsx(InputStream io) throws Exception {
        try {
            ArrayList<ArrayList<String>> dataList = new ArrayList<ArrayList<String>>();
            ArrayList<String> titleList = new ArrayList<String>();

            XSSFWorkbook xssfWorkbook;
            xssfWorkbook = new XSSFWorkbook(io);
            XSSFSheet xssfSheet = xssfWorkbook.getSheetAt(0);

            int rowstart = xssfSheet.getFirstRowNum();
            int rowEnd = xssfSheet.getLastRowNum();

            // 分离excel第一行
            XSSFRow row1 = xssfSheet.getRow(rowstart);
            if (null == row1) {
                xssfWorkbook.close();
                return null;
            }
            int cellStart1 = row1.getFirstCellNum();
            int cellEnd1 = row1.getLastCellNum();
            for (int k = cellStart1; k <= cellEnd1; k++) {
                XSSFCell cell = row1.getCell(k);
                if (null == cell) {
                    titleList.add("");
                } else {
                    titleList.add(cell.toString());
                }
            }

            for (int i = rowstart + 1; i <= rowEnd; i++) {

                XSSFRow row = xssfSheet.getRow(i);
                if (null == row) {
                    continue;
                }
                int cellStart = row.getFirstCellNum();
                int cellEnd = row.getLastCellNum();
                ArrayList<String> arrayList = new ArrayList<String>();
                for (int k = cellStart; k <= cellEnd; k++) {
                    XSSFCell cell = row.getCell(k);
                    if (null == cell) {
                        arrayList.add("");
                    } else {
                        switch (cell.getCellTypeEnum()) {
                            case NUMERIC: // 数字
                                arrayList.add(NumberToTextConverter.toText(cell.getNumericCellValue()));
                                break;
                            case STRING: // 字符串
                                arrayList.add(cell.getStringCellValue().trim());
                                break;
                            case BOOLEAN: // Boolean
                                arrayList.add(String.valueOf(cell.getBooleanCellValue()));
                                break;
                            case FORMULA: // 公式
                                arrayList.add(cell.getCellFormula());
                                break;
                            case BLANK: // 空值
                                arrayList.add("");
                                break;
                            case ERROR: // 故障
                                arrayList.add("");
                                break;
                            default:
                                arrayList.add("");
                                break;
                        }
                    }
                }

                dataList.add(arrayList);
            }
            ExcelOutPut excelOutPut = new ExcelOutPut(titleList, dataList);
            xssfWorkbook.close();
            return excelOutPut;
        } catch (Exception e) {
            throw new Exception("导入失败，请重试！" + e.toString() + "&&&&&&" + e.getMessage());
        }
    }

    /**
     * 读取xls文件
     *
     * @param io 文件绝对路径
     * @throws Exception
     */
    public static ExcelOutPut readXls(InputStream io) throws Exception {
        try {
            ArrayList<ArrayList<String>> dataList = new ArrayList<ArrayList<String>>();
            ArrayList<String> titleList = new ArrayList<String>();

            HSSFWorkbook hssfWorkbook = new HSSFWorkbook(io);
            HSSFSheet hssfSheet = hssfWorkbook.getSheetAt(0);

            int rowstart = hssfSheet.getFirstRowNum();
            int rowEnd = hssfSheet.getLastRowNum();

            // 分离excel第一行
            HSSFRow row1 = hssfSheet.getRow(rowstart);
            if (null == row1) {
                hssfWorkbook.close();
                return null;
            }
            int cellStart1 = row1.getFirstCellNum();
            int cellEnd1 = row1.getLastCellNum();
            for (int k = cellStart1; k <= cellEnd1; k++) {
                HSSFCell cell = row1.getCell(k);
                if (null == cell) {
                    titleList.add("");
                } else {
                    titleList.add(cell.toString());
                }
            }

            for (int i = rowstart + 1; i <= rowEnd; i++) {
                HSSFRow row = hssfSheet.getRow(i);
                if (null == row) {
                    continue;
                }
                int cellStart = row.getFirstCellNum();
                int cellEnd = row.getLastCellNum();
                ArrayList<String> arrayList = new ArrayList<String>();
                for (int k = cellStart; k <= cellEnd; k++) {
                    HSSFCell cell = row.getCell(k);
                    if (null == cell) {
                        arrayList.add("");
                    } else {
                        switch (cell.getCellTypeEnum()) {
                            case NUMERIC: // 数字
                                arrayList.add(NumberToTextConverter.toText(cell.getNumericCellValue()));
                                break;
                            case STRING: // 字符串
                                arrayList.add(cell.getStringCellValue().trim());
                                break;
                            case BOOLEAN: // Boolean
                                arrayList.add(String.valueOf(cell.getBooleanCellValue()));
                                break;
                            case FORMULA: // 公式
                                arrayList.add(cell.getCellFormula());
                                break;
                            case BLANK: // 空值
                                arrayList.add("");
                                break;
                            case ERROR: // 故障
                                arrayList.add("");
                                break;
                            default:
                                arrayList.add("");
                                break;
                        }
                    }
                    dataList.add(arrayList);
                }
            }
            ExcelOutPut excelOutPut = new ExcelOutPut(titleList, dataList);
            hssfWorkbook.close();
            return excelOutPut;
        } catch (Exception e) {
            throw new Exception("导入失败，请重试！" + e.toString() + "&&&&&&" + e.getMessage());
        }

    }

    /**
     * 使用POI创建excel工作簿
     *
     * @param response
     * @param excelOutPut 输入数据
     * @throws IOException
     */
    public static boolean createExcel(HttpServletResponse response, ExcelOutPut excelOutPut) throws IOException {

        ArrayList<String> titleList = excelOutPut.getTitleList();
        ArrayList<ArrayList<String>> dataList = excelOutPut.getDataList();
        // 创建excel工作簿
        XSSFWorkbook wb = new XSSFWorkbook();
        // 创建第一个sheet（页），命名为 new sheet
        XSSFSheet sheet = wb.createSheet();
        // wb.setSheetName(0, "sheet1");// 工作簿名称
        // Row 行
        // Cell 方格
        // Row 和 Cell 都是从0开始计数的

        // 创建一行，在页sheet上
        XSSFRow row = sheet.createRow((short) 0);
        for (int i = 0; i < titleList.size(); i++) {
            // 在row行上创建一个方格,设置方格的显示
            row.createCell(i).setCellValue(titleList.get(i));
        }

        for (int i = 0; i < dataList.size(); i++) {
            row = sheet.createRow((short) i + 1);
            for (int j = 0; j < dataList.get(i).size(); j++) {
                row.createCell(j).setCellValue(dataList.get(i).get(j));
            }
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date();
        // 下载文件的默认名称
        response.setHeader("Content-Disposition", "attachment;filename=" + formatter.format(date) + ".xlsx");
        // 告诉浏览器用什么软件可以打开此文件
        response.setContentType("application/octet-stream;charset=utf-8");
        OutputStream outStream = response.getOutputStream();

        wb.write(outStream);
        wb.close();
        return true;
    }
}


