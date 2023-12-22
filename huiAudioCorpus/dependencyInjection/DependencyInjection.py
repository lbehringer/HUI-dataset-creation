def disableLog():
    logging.getLogger('matplotlib').disabled = True
    logging.getLogger('matplotlib.font_manager').disabled = True
    logging.getLogger('matplotlib.colorbar').disabled = True
    logging.getLogger('numba.core.ssa').disabled = True
    logging.getLogger('numba.core.interpreter').disabled = True
    logging.getLogger('numba.core.byteflow').disabled = True
    logging.getLogger('numba.ssa').disabled = True
    logging.getLogger('numba.byteflow').disabled = True
    logging.getLogger('numba.interpreter').disabled = True
    logging.getLogger('paramiko.transport.sftp').disabled = True
    logging.getLogger('paramiko.transport').disabled = True
    logging.getLogger('h5py._conv').disabled = True
    logging.getLogger().setLevel(logging.WARNING)
    
from huiAudioCorpus.workflows.createDatasetWorkflow.Step8_DatasetStatistic import Step8_DatasetStatistic
from huiAudioCorpus.workflows.createDatasetWorkflow.Step9_GenerateCleanDataset import Step9_GenerateCleanDataset
from huiAudioCorpus.workflows.createDatasetWorkflow.Step7_AudioRawStatistic import Step7_AudioRawStatistic
from huiAudioCorpus.workflows.createDatasetWorkflow.Step3_1_PrepareText import Step3_1_PrepareText
from huiAudioCorpus.workflows.createDatasetWorkflow.Step0_Overview import Step0_Overview
from huiAudioCorpus.workflows.createDatasetWorkflow.Step2_1_AudioStatistic import Step2_1_AudioStatistic
from huiAudioCorpus.workflows.createDatasetWorkflow.Step6_FinalizeDataset import Step6_FinalizeDataset
from huiAudioCorpus.transformer.SentenceDistanceTransformer import SentenceDistanceTransformer
from huiAudioCorpus.calculator.AlignSentencesIntoTextCalculator import AlignSentencesIntoTextCalculator
from huiAudioCorpus.workflows.createDatasetWorkflow.Step5_AlignText import Step5_AlignText
from huiAudioCorpus.converter.AudioToSentenceConverter import AudioToSentenceConverter
from huiAudioCorpus.workflows.createDatasetWorkflow.Step4_TranscriptAudio import Step4_TranscriptAudio
from huiAudioCorpus.persistenz.GutenbergBookPersistenz import GutenbergBookPersistenz
from huiAudioCorpus.workflows.createDatasetWorkflow.Step3_DownloadText import Step3_DownloadText
from huiAudioCorpus.transformer.AudioSplitTransformer import AudioSplitTransformer
from huiAudioCorpus.transformer.AudioLoudnessTransformer import AudioLoudnessTransformer
from huiAudioCorpus.workflows.createDatasetWorkflow.Step2_SplitAudio import Step2_SplitAudio
from huiAudioCorpus.persistenz.AudiosFromLibrivoxPersistenz import AudiosFromLibrivoxPersistenz
from huiAudioCorpus.workflows.createDatasetWorkflow.Step1_DownloadAudio import Step1_DownloadAudio
from huiAudioCorpus.converter.StringToSentencesConverter import StringToSentencesConverter
from huiAudioCorpus.workflows.createDatasetWorkflow.QA1_HifiBandwidth import QA1_HifiBandwidth
from huiAudioCorpus.workflows.createDatasetWorkflow.QA2_HifiSNR import QA2_HifiSNR
from huiAudioCorpus.workflows.createDatasetWorkflow.QA3_WVMOS import QA3_WVMOS
from huiAudioCorpus.workflows.createDatasetWorkflow.Step4_1_NormalizeTranscript import Step4_1_NormalizeTranscript

from frosch import hook
hook(theme = 'paraiso_dark')
import logging
disableLog()

from huiAudioCorpus.error.DependencyInjectionError import DependencyInjectionError
from huiAudioCorpus.converter.ListToHistogramConverter import ListToHistogramConverter
from huiAudioCorpus.converter.ListToStatisticConverter import ListToStatisticConverter
from huiAudioCorpus.ui.Plot import Plot
from huiAudioCorpus.components.TextStatisticComponent import TextStatisticComponent
from huiAudioCorpus.components.AudioStatisticComponent import AudioStatisticComponent
from huiAudioCorpus.utils.PathUtil import PathUtil
from huiAudioCorpus.utils.FileListUtil import FileListUtil
from huiAudioCorpus.converter.TranscriptsToSentencesConverter import TranscriptsToSentencesConverter
from huiAudioCorpus.persistenz.AudioTranscriptPairPersistenz import AudioTranscriptPairPersistenz
from huiAudioCorpus.converter.PhoneticSentenceToSymbolSentenceConverter import PhoneticSentenceToSymbolSentenceConverter
from huiAudioCorpus.converter.SentenceToPhoneticSentenceConverter import SentenceToPhoneticSentenceConverter
from huiAudioCorpus.transformer.AudioAddSilenceTransformer import AudioAddSilenceTransformer
from huiAudioCorpus.transformer.TranscriptsSelectionTransformer import TranscriptsSelectionTransformer
from huiAudioCorpus.transformer.AudioSamplingRateTransformer import AudioSamplingRateTransformer
from huiAudioCorpus.persistenz.TranscriptsPersistenz import TranscriptsPersistenz
from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.filter.AudioFilter import AudioFilter
from huiAudioCorpus.transformer.AudioFadeTransformer import AudioFadeTransformer
from huiAudioCorpus.calculator.TextNormalizer import TextNormalizer

import inspect

disableLog()
default_config = {
    'audio_add_silence_transformer': {
        'end_duration_seconds': 0.7,
        'start_duration_seconds': 0
    },
    'list_to_histogram_converter': {
        'step_size': 1
    }
}

class DependencyInjection:
    # assign all the __annotations__ to DependencyInjection.__dict__
    # Calculators
    align_sentences_into_text_calculator: AlignSentencesIntoTextCalculator
    text_normalizer: TextNormalizer

    # Components
    audio_statistic_component: AudioStatisticComponent
    text_statistic_component: TextStatisticComponent

    # Converters
    phonetic_sentence_to_symbol_sentence_converter: PhoneticSentenceToSymbolSentenceConverter
    sentence_to_phonetic_sentence_converter: SentenceToPhoneticSentenceConverter
    transcripts_to_sentences_converter: TranscriptsToSentencesConverter
    list_to_statistic_converter: ListToStatisticConverter
    list_to_histogram_converter: ListToHistogramConverter
    string_to_sentences_converter: StringToSentencesConverter
    audio_to_sentence_converter: AudioToSentenceConverter

    # Filters
    audio_filter: AudioFilter

    # Persistence
    audio_persistenz: AudioPersistenz
    audio_persistenz: AudioPersistenz
    audio_transcript_pair_persistenz: AudioTranscriptPairPersistenz
    transcripts_persistenz: TranscriptsPersistenz
    audios_from_librivox_persistenz: AudiosFromLibrivoxPersistenz
    gutenberg_book_persistenz: GutenbergBookPersistenz

    # Transformers
    audio_add_silence_transformer: AudioAddSilenceTransformer
    audio_sampling_rate_transformer: AudioSamplingRateTransformer
    audio_sr_transformer: AudioSamplingRateTransformer
    transcripts_selection_transformer: TranscriptsSelectionTransformer
    audio_split_transformer: AudioSplitTransformer
    sentence_distance_transformer: SentenceDistanceTransformer
    audio_loudness_transformer: AudioLoudnessTransformer
    audio_loudness_transformer: AudioLoudnessTransformer
    audio_fade_transformer: AudioFadeTransformer

    # Utilities
    path_util: PathUtil
    file_list_util: FileListUtil

    # Workflows
    step0_overview: Step0_Overview
    step1_download_audio: Step1_DownloadAudio
    step2_split_audio: Step2_SplitAudio
    step2_1_audio_statistic: Step2_1_AudioStatistic
    step3_download_text: Step3_DownloadText
    step3_1_prepare_text: Step3_1_PrepareText
    step4_transcript_audio: Step4_TranscriptAudio
    step4_1_normalize_transcript: Step4_1_NormalizeTranscript
    step5_align_text: Step5_AlignText
    step6_finalize_dataset: Step6_FinalizeDataset
    step7_audio_raw_statistic: Step7_AudioRawStatistic
    step8_dataset_statistic: Step8_DatasetStatistic
    step9_generate_clean_dataset: Step9_GenerateCleanDataset
    qa1_hifi_bandwidth: QA1_HifiBandwidth
    qa2_hifi_snr: QA2_HifiSNR
    qa3_wvmos: QA3_WVMOS

    # Plot
    plot: Plot

    def __init__(self, config={}):
        config_with_default = default_config.copy()
        config_with_default.update(config)
        self.all_class_references = self.get_all_class_references(config_with_default)
        initialed_classes = {}
        for name, class_instance in self.all_class_references.items():
            def get_lambda(name, class_instance):
                return property(lambda _: self.init_class(name, class_instance, self.class_constructor, initialed_classes, config_with_default, name))
            setattr(DependencyInjection, name, get_lambda(name, class_instance))

    def init_class(self, class_name, class_reference, class_constructor_method, initialed_classes, config, requested_class=''):
        if class_name in initialed_classes:
            return initialed_classes[class_name]
        arguments = self.get_constructor_reference_classes(class_reference)
        for argument in arguments:
            if argument not in initialed_classes.values() and arguments[argument] is not None:
                self.init_class(argument, arguments[argument], class_constructor_method, initialed_classes, config, requested_class)

        class_config = config[class_name].copy() if class_name in config else {}
        if '#' in class_config:
            class_config.pop('#')
        class_config
        try:
            new_class_instance = class_constructor_method(class_reference, initialed_classes, class_config)
        except Exception as e:
            raise DependencyInjectionError(e, class_config, class_reference.__name__, requested_class)
        initialed_classes[class_name] = new_class_instance
        return new_class_instance

    def class_constructor(self, class_reference, initialed_classes, class_config):
        class_constructor = class_config.copy()
        references = self.get_constructor_reference_classes(class_reference)
        for ref in references:
            if references[ref] is not None:
                class_constructor[ref] = initialed_classes[ref]
        class_instance = class_reference(**class_constructor)

        return class_instance

    def get_constructor_reference_classes(self, class_reference):
        arguments = self.get_all_constructor_arguments(class_reference)

        references = {}
        for argument in arguments:
            if argument in ["self", "args", "kwargs"]:
                continue
            references[argument] = self.all_class_references[argument] if argument in self.all_class_references.keys() else None
        return references

    def get_all_constructor_arguments(self, class_instance):
        return list(inspect.signature(class_instance.__init__).parameters.keys())

    def get_all_class_references(self, config_with_default):
        classes = global_classes_at_import_time.copy()
        for class_name in config_with_default:
            if '#' in config_with_default[class_name]:
                classes[class_name] = config_with_default[class_name]['#']
        return classes

global_classes_at_import_time = DependencyInjection.__dict__.get("__annotations__")
