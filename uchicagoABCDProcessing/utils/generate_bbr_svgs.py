from niworkflows.viz.utils import compose_view, plot_segs
import glob
import nibabel
import os
from pathlib import Path

def generate_flirt_bbr_svg(subject: str, task: str, run: str):
    anat_img = nibabel.load("/cds2/abcd/cold/derivatives/fmriprep/%s/anat/%s_space-MNI152NLin6Asym_desc-preproc_T1w.nii.gz" % (subject, subject))
    mask_img = nibabel.load('/cds2/abcd/cold/derivatives/fmriprep/%s/anat/%s_space-MNI152NLin6Asym_desc-brain_mask.nii.gz' % (subject, subject))
    anat_data = anat_img.get_data()
    mask_data = mask_img.get_data()
    anat_data[mask_data==0] = 0.
    nibabel.Nifti1Image(anat_data,anat_img.affine,header=anat_img.header).to_filename('tmp_anat.nii.gz')

    anat = plot_segs(
        image_nii='tmp_anat.nii.gz',
        seg_niis=glob.glob('/cds2/abcd/cold/derivatives/fmriprep/%s/anat/%s_space-MNI152NLin6Asym_*probseg.nii.gz' % (subject, subject)),
        bbox_nii='/cds2/abcd/cold/derivatives/fmriprep/%s/anat/%s_space-MNI152NLin6Asym_desc-brain_mask.nii.gz' % (subject, subject),
        masked=False,
        out_file='anat.svg'
              )

    func_img = nibabel.load("/cds2/abcd/cold/derivatives/fmriprep/%s/ses-baselineYear1Arm1/func/%s_ses-baselineYear1Arm1_task-%s_run-%s_space-MNI152NLin6Asym_boldref.nii.gz" % (subject, subject, task, run))
    mask_img = nibabel.load('/cds2/abcd/cold/derivatives/fmriprep/%s/ses-baselineYear1Arm1/func/%s_ses-baselineYear1Arm1_task-%s_run-%s_space-MNI152NLin6Asym_desc-brain_mask.nii.gz'  % (subject, subject, task, run))
    func_data = func_img.get_data()
    mask_data = mask_img.get_data()
    func_data[mask_data==0] = 0.
    nibabel.Nifti1Image(func_data,func_img.affine,header=func_img.header).to_filename('tmp_func.nii.gz')

    func = plot_segs(
        image_nii="tmp_func.nii.gz",
        seg_niis=glob.glob('/cds2/abcd/cold/derivatives/fmriprep/%s/anat/%s_space-MNI152NLin6Asym_*probseg.nii.gz' % (subject, subject)),
        bbox_nii='/cds2/abcd/cold/derivatives/fmriprep/%s/ses-baselineYear1Arm1/func/%s_ses-baselineYear1Arm1_task-%s_run-%s_space-MNI152NLin6Asym_desc-brain_mask.nii.gz' % (subject, subject, task, run),
        masked=False,
        out_file='func.svg'
              )

    out_file = "/cds2/abcd/cold/derivatives/fmriprep/%s/ses-baselineYear1Arm1/func/%s_ses-baselineYear1Arm1_task-%s_run-%s_desc-flirtbbr_bold.svg" % (subject, subject, task, run)
    out_file_duplicate = "/cds2/abcd/cold/derivatives/fmriprep_duplicates/%s/ses-baselineYear1Arm1/func/%s_ses-baselineYear1Arm1_task-%s_run-%s_desc-flirtbbr_bold.svg" % (subject, subject, task, run)
    os.makedirs(str(Path(out_file_duplicate).parent),exist_ok=True)
    compose_view(bg_svgs=anat,fg_svgs=func,out_file=out_file)
    compose_view(bg_svgs=anat, fg_svgs=func, out_file=out_file_duplicate)

    os.system('rm tmp_anat.nii.gz')
    os.system('rm tmp_func.nii.gz')
    os.system('chmod 777 %s' % out_file)
    os.system('chmod 777 %s' % out_file_duplicate)