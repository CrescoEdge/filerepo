package io.cresco.filerepo;

import java.io.File;

public class FileObject {

    public String fileName;
    public String MD5;
    public String filePath;
    public long lastModified;
    public long filesize;

    public FileObject(String fileName, String MD5, String filePath, long lastModified, long filesize) {
        this.fileName = fileName;
        this.MD5 = MD5;
        this.filePath = filePath;
        this.lastModified = lastModified;
        this.filesize = filesize;
    }

}
