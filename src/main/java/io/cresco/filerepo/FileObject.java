package io.cresco.filerepo;

import java.io.File;

public class FileObject {

    public String fileName;
    public String MD5;
    public String filePath;
    public long lastModified;

    public FileObject(String fileName, String MD5, String filePath, long lastModified) {
        this.fileName = fileName;
        this.MD5 = MD5;
        this.filePath = filePath;
        this.lastModified = lastModified;
    }

}
