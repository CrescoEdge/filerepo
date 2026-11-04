package io.cresco.filerepo;

/**
 * A delete a producer offers its subscribers (FR-D4): the file it no longer holds, the name a subscriber holds it under
 * (relative to the repository directory) and the content it had. A subscriber deletes its copy only if that copy is
 * this exact content, so a delete never removes a file the producer did not have.
 */
public class DeleteObject {

    public String fileName;
    public String MD5;
    public String filePath;
    public long deletedAt;

    public DeleteObject(String fileName, String MD5, String filePath, long deletedAt) {
        this.fileName = fileName;
        this.MD5 = MD5;
        this.filePath = filePath;
        this.deletedAt = deletedAt;
    }
}
