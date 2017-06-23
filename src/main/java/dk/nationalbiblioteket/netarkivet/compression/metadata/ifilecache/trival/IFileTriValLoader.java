/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trival;

import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.AlreadyKnownMissingFileException;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileIOHelper;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileMap;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileMapLoader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IFileTriValLoader implements IFileMapLoader {

    private final List<String> knownMissing = new ArrayList<>();

    @Override
    public IFileMap getIFileMap(String filename) throws FileNotFoundException {
        synchronized (knownMissing) {
            if (knownMissing.contains(filename)) {
                throw new AlreadyKnownMissingFileException();
            }
        }
        try {
            IFileIOHelper.IFileArrays ifArrays = IFileIOHelper.loadAsArrays(filename);
            return new IFileTriValMap(
                    filename, ifArrays.getKeys(), ifArrays.getValues1(), ifArrays.getValues2(), ifArrays.size());
        } catch (FileNotFoundException e1) {
            knownMissing.add(filename);
            throw (e1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
