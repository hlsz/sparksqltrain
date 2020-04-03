package com.data.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Random;

public class PDFService {
    private final Logger logger = LoggerFactory.getLogger(PDFService.class);
    private Random random = new Random();
    public byte[] html2pdf(String srcUrl) {
        String tmp = System.getProperty("java.io.tmpdir");
        String filename = System.currentTimeMillis() + random.nextInt(1000) + "";
        return html2pdf(srcUrl, tmp + "/" + filename);
    }
    private byte[] html2pdf(String src, String dest) {
        String space = " ";
        String wkhtmltopdf = findExecutable();
        if (StringUtils.isEmpty(wkhtmltopdf)) {
            logger.error("no wkhtmltopdf found!");
            throw new RuntimeException("html转换pdf出错了");
        }
        File file = new File(dest);
        File parent = file.getParentFile();
        if (!parent.exists()) {
            boolean dirsCreation = parent.mkdirs();
            logger.info("create dir for new file,{}", dirsCreation);
        }
        StringBuilder cmd = new StringBuilder();
        cmd.append(findExecutable()).append(space)
                .append(src).append(space)
                .append("--footer-right").append(space).append("页码:[page]").append(space)
                .append("--footer-font-size").append(space).append("5").append(space)
                .append("--disable-smart-shrinking").append(space)
                .append("--load-media-error-handling")
                .append(space).append("ignore").append(space)
                .append("--load-error-handling").append(space).append("ignore").append(space)
                .append("--footer-left").append(space).append("电子档打印时间:[date]").append(space)
                .append(dest);
        InputStream is = null;
        try {
            String finalCmd = cmd.toString();
            logger.info("final cmd:{}", finalCmd);
            Process proc = Runtime.getRuntime().exec(finalCmd);
            new Thread(new ProcessStreamHandler(proc.getInputStream())).start();
            new Thread(new ProcessStreamHandler(proc.getErrorStream())).start();
            proc.waitFor();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            is = new FileInputStream(file);
            byte[] buf = new byte[1024];
            while (is.read(buf, 0, buf.length) != -1) {
                baos.write(buf, 0, buf.length);
            }
            return baos.toByteArray();
        } catch (Exception e) {
            logger.error("html转换pdf出错", e);
            throw new RuntimeException("html转换pdf出错了");
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * Attempts to find the `wkhtmltopdf` executable in the system path.
     *
     * @return the wkhtmltopdf command according to the OS
     */
    public String findExecutable() {
        Process p;
        try {
            String osname = System.getProperty("os.name").toLowerCase();
            String cmd = osname.contains("windows") ? "where wkhtmltopdf" : "which wkhtmltopdf";
            p = Runtime.getRuntime().exec(cmd);
            new Thread(new ProcessStreamHandler(p.getErrorStream())).start();
            p.waitFor();
            return IOUtils.toString(p.getInputStream(), Charset.defaultCharset());
        } catch (Exception e) {
            logger.error("no wkhtmltopdf found!", e);
        }
        return "";
    }
    private static class ProcessStreamHandler implements Runnable {
        private InputStream is;
        public ProcessStreamHandler(InputStream is) {
            this.is = is;
        }
        @Override
        public void run() {
            BufferedReader reader = null;
            try {
                InputStreamReader isr = new InputStreamReader(is, "utf-8");
                reader = new BufferedReader(isr);
                String line;
                while ((line = reader.readLine()) != null) {
                    if (ConfigAdapter.getIsDebug())
                        System.out.println(line); //输出内容
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
