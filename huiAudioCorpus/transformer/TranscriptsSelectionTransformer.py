from typing import List
from huiAudioCorpus.model.Transcripts import Transcripts

class TranscriptsSelectionTransformer:

    def transform(self, transcripts: Transcripts, selected_keys: List[str]):
        trans = transcripts.transcripts
        transformed_trans = trans[trans[0].isin(selected_keys)]  # type:ignore
        transformed_transcripts = Transcripts(transformed_trans, transcripts.id, transcripts.name)
        return transformed_transcripts
