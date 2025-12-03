///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.eclipse.jgit:org.eclipse.jgit:7.4.0.202509020913-r

import static java.lang.System.*;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.util.io.DisabledOutputStream;

public class list_git_commit {

    public static void main(String... args) throws IOException, NoHeadException, GitAPIException {
        String repoPath = "./";
        try (Git git = Git.open(new File(repoPath));
                ObjectReader reader = git.getRepository().newObjectReader();
                DiffFormatter diffFormatter = new DiffFormatter(DisabledOutputStream.INSTANCE);) {
            diffFormatter.setRepository(git.getRepository());
            Iterator<RevCommit> it = git.log().call().iterator();
            while (it.hasNext()) {
                RevCommit rc = it.next();

                List<DiffEntry> changes = git
                        .diff()
                        .setOldTree(
                                rc.getParentCount() > 0
                                        ? new CanonicalTreeParser(null, reader, rc.getParent(0).getTree().getId())
                                        : null)
                        .setNewTree(new CanonicalTreeParser(null, reader, rc.getTree().getId()))
                        .call();

                for (DiffEntry change : changes) {
                    EditList el = diffFormatter.toFileHeader(change).toEditList();
                    List<Integer> deletedAndInserted = el.stream()
                            .map(edit -> List.of(edit.getLengthA(), edit.getLengthB())).reduce(List.of(0, 0),
                                    (l1, l2) -> List.of(l1.get(0) + l2.get(0), l1.get(1) + l2.get(1)));
                    int deleted = deletedAndInserted.get(0);
                    int inserted = deletedAndInserted.get(1);
                    out.println(
                            String.format("%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%s", rc.getId().getName(),
                                    rc.getShortMessage(),
                                    rc.getAuthorIdent().getEmailAddress(),
                                    rc.getCommitterIdent().getEmailAddress(), rc.getCommitTime(), rc.getCommitTime(),
                                    change.getOldPath(), change.getNewPath(), deleted, inserted,
                                    change.getChangeType()));
                }
            }
        }
    }
}
