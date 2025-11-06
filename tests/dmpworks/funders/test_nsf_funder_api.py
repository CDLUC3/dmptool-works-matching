import os

import vcr

from dmpworks.funders.nsf_funder_api import (
    extract_doi,
    find_crossref_doi,
    find_datacite_doi,
    nsf_fetch_award_publication_dois,
    nsf_fetch_org_id,
    parse_reference,
)
from queries.dmpworks.tests.utils import get_fixtures_path

FIXTURES_FOLDER = get_fixtures_path()


def test_nsf_fetch_award_publications():
    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nsf_fetch_award_publication_dois_1507101.yaml")):
        results = nsf_fetch_award_publication_dois("1507101")
        dois = [result["doi"] for result in results]
        assert [
            "10.1016/j.surfrep.2018.08.001",
            "10.1063/1.5143061",
            "10.1063/1.5143061",
            "10.1038/ncomms8358",
            "10.1016/j.susc.2014.11.016",
            "10.1016/j.susc.2016.11.003",
            "10.1016/j.susc.2016.11.003",
            "10.1021/acs.nanolett.8b02123",
            "10.1021/acs.jpcc.9b12023",
            "10.1021/acs.jpcc.9b12023",
            "10.1021/acs.jpcc.0c07517",
            "10.1021/acs.jpcc.0c07316",
            "10.1021/acs.jctc.1c00630",
            "10.1063/5.0048920",
            "10.3390/cryst9040218",
            "10.1021/acs.inorgchem.8b00281",
            "10.3390/cryst9040218",
            "10.1021/acs.inorgchem.8b00281",
            "10.1016/j.surfrep.2018.08.001",
            "10.1021/acs.nanolett.8b02123",
        ] == dois

    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nsf_fetch_award_publication_dois_0802290.yaml")):
        results = nsf_fetch_award_publication_dois("0802290")
        dois = [result["doi"] for result in results]
        assert [
            "10.1016/j.dsr2.2012.02.002",
            "10.3354/meps08275",
            "10.3354/meps08275",
            "10.5670/oceanog.2009.105",
            "10.114/annurev-marine-120710-100926",
            "10.1029/2010eo180001",
            "10.1007/s00300-011-1151-6",
            "10.14430/arctic646",
            "10.14430/arctic646",
            "10.1890/08-1193.1",
            "10.1890/08-1193.1",
            "10.1007/s12526-010-0059-7",
            "10.1007/s12526-010-0059-7",
            None,  # 10.1111/j.1748-7692.2009.00316.x: this one doesn't match because the title is too different "Pacific walrus (Odobenus rosmarus divergens): Differential prey digestion and diet"
            "10.1007/s00300-010-0947-0",
            "10.1007/s00300-010-0947-0",
            "10.1007/s00300-011-1044-8",
        ] == dois


def test_find_crossref_doi():
    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "find_crossref_doi.yaml")):
        doi = find_crossref_doi(
            "Usability Analysis of Visual Programming Environments: A ‘Cognitive Dimensions’ Framework",
            "Journal of Visual Languages & Computing",
        )
        assert "10.1006/jvlc.1996.0009" == doi

        doi = find_crossref_doi(
            "The “Physics” of Notations: Toward a Scientific Basis for Constructing Visual Notations in Software Engineering",
            "IEEE Transactions on software engineering",
        )
        assert "10.1109/tse.2009.67" == doi


def test_find_datacite_doi():
    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "find_datacite_doi.yaml")):
        doi = find_datacite_doi("COKI Open Access Dataset")
        assert "10.5281/zenodo.6399462" == doi

        doi = find_datacite_doi("The Data for Cloud Removal in Full-disk Solar Images Using Deep Learning")
        assert "10.5281/zenodo.13841240" == doi

        doi = find_datacite_doi(
            'Supplemental Figures for: "The SDSS-V Black Hole Mapper Reverberation Mapping Project: Multi-Line Dynamical Modeling of a Highly Variable Active Galactic Nucleus with Decade-long Light Curves"'
        )
        assert "10.5281/zenodo.13259639" == doi

        doi = find_datacite_doi("tidyterra: 'tidyverse' Methods and 'ggplot2' Helpers for 'terra' Objects")
        assert "10.5281/zenodo.6572471" == doi


def test_parse_reference():
    # Test parsing real world data
    rows = [
        (
            "Surface Science Reports~2018~73~Andersen, T. K. and Fong, D. D. and Marks, L. D.~10.1016/j.surfrep.2018.08.001~213-232~Pauling's rules for oxide surfaces~2021-12-06 08:27:13.943",
            dict(
                doi="10.1016/j.surfrep.2018.08.001",
                journal="Surface Science Reports",
                year="2018",
                title="Pauling's rules for oxide surfaces",
            ),
        ),
        (
            "The Journal of Chemical Physics~2020~152~Blaha, Peter and Schwarz, Karlheinz and Tran, Fabien and Laskowski, Robert and Madsen, Georg K. H. and Marks, Laurence D.~10.1063/1.5143061~074101~WIEN2k: An APW+lo program for calculating the properties of solids~2021-12-06 08:27:13.966",
            dict(
                doi="10.1063/1.5143061",
                journal="The Journal of Chemical Physics",
                year="2020",
                title="WIEN2k: An APW+lo program for calculating the properties of solids",
            ),
        ),
        (
            "The Journal of Chemical Physics~2020~152~Blaha, Peter and Schwarz, Karlheinz and Tran, Fabien and Laskowski, Robert and Madsen, Georg K. H. and Marks, Laurence D.~10.1063/1.5143061~WIEN2k: An APW+lo program for calculating the properties of solids~2020-06-04 15:22:13.726",
            dict(
                doi="10.1063/1.5143061",
                journal="The Journal of Chemical Physics",
                year="2020",
                title="WIEN2k: An APW+lo program for calculating the properties of solids",
            ),
        ),
        (
            "Nat Commun~2015~6~Ciston, J. and Brown, H. G. and D'Alfonso, A. J. and Koirala, P. and Ophus, C. and Lin, Y. and Suzuki, Y. and Inada, H. and Zhu, Y. and Allen, L. J. and Marks, L. D.~10.1038/ncomms8358~7358~Surface determination through atomically resolved secondary-electron imaging~2016-06-08 13:13:39.046",
            dict(
                doi="10.1038/ncomms8358",
                journal="Nat Commun",
                year="2015",
                title="Surface determination through atomically resolved secondary-electron imaging",
            ),
        ),
        (
            "Surface Science~2015~633~Kienzle, D. and Koirala, P. and Marks, L. D.~10.1016/j.susc.2014.11.016~60-67~Lanthanum aluminate (110) 3 x 1 surface reconstruction~2016-06-08 13:13:39.063",
            dict(
                doi="10.1016/j.susc.2014.11.016",
                journal="Surface Science",
                year="2015",
                title="Lanthanum aluminate (110) 3 x 1 surface reconstruction",
            ),
        ),
        (
            "Surface Science~2016~657~Koirala, P. and Gulec, A. and Marks, L. D.~10.1016/j.susc.2016.11.003~15~Surface heterogeneity in KTaO3~2017-05-17 11:23:31.813",
            dict(
                doi="10.1016/j.susc.2016.11.003",
                journal="Surface Science",
                year="2016",
                title="Surface heterogeneity in KTaO3",
            ),
        ),
        (
            "Surface Science~2017~657~Koirala, P. and Gulec, A. and Marks, L. D.~10.1016/j.susc.2016.11.003~15-19~Surface heterogeneity in KTaO3 (001)~2021-12-06 08:27:13.98",
            dict(
                doi="10.1016/j.susc.2016.11.003",
                journal="Surface Science",
                year="2017",
                title="Surface heterogeneity in KTaO3 (001)",
            ),
        ),
        (
            "Nano Lett~2018~18~Ly, T. and Wen, J. and Marks, L. D.~10.1021/acs.nanolett.8b02123~5186-5191~Kinetic Growth Regimes of Hydrothermally Synthesized Potassium Tantalate Nanoparticles~2021-12-06 08:27:13.983",
            dict(
                doi="10.1021/acs.nanolett.8b02123",
                journal="Nano Lett",
                year="2018",
                title="Kinetic Growth Regimes of Hydrothermally Synthesized Potassium Tantalate Nanoparticles",
            ),
        ),
        (
            "The Journal of Physical Chemistry C~2020~124~Ly, Tiffany and Wen, Jianguo and Marks, Laurence D.~10.1021/acs.jpcc.9b12023~7988-7993~Chemisorption-Driven Roughening of Hydrothermally Grown KTa1xNbxO3 Nanoparticles~2020-06-04 15:22:13.763",
            dict(
                doi="10.1021/acs.jpcc.9b12023",
                journal="The Journal of Physical Chemistry C",
                year="2020",
                title="Chemisorption-Driven Roughening of Hydrothermally Grown KTa1xNbxO3 Nanoparticles",
            ),
        ),
        (
            "The Journal of Physical Chemistry C~2020~124~Ly, Tiffany and Wen, Jianguo and Marks, Laurence D.~10.1021/acs.jpcc.9b12023~7988-7993~Chemisorption-Driven Roughening of Hydrothermally Grown KTa1xNbxO3 Nanoparticles~2021-12-06 08:27:13.993",
            dict(
                doi="10.1021/acs.jpcc.9b12023",
                journal="The Journal of Physical Chemistry C",
                year="2020",
                title="Chemisorption-Driven Roughening of Hydrothermally Grown KTa1xNbxO3 Nanoparticles",
            ),
        ),
        (
            "The Journal of Physical Chemistry C~2020~124~Ly, Tiffany and Wen, Jianguo and Marks, Laurence D.~10.1021/acs.jpcc.0c07517~26012-260~Complex Fluorine Chemical Potential Effects on the Shape and Compositional Heterogeneity of KTa1xNbxO3 Nanoparticles~2021-12-06 08:27:13.996",
            dict(
                doi="10.1021/acs.jpcc.0c07517",
                journal="The Journal of Physical Chemistry C",
                year="2020",
                title="Complex Fluorine Chemical Potential Effects on the Shape and Compositional Heterogeneity of KTa1xNbxO3 Nanoparticles",
            ),
        ),
        (
            "The Journal of Physical Chemistry C~2020~124~Mansley, Zachary R. and Marks, Laurence D.~10.1021/acs.jpcc.0c07316~28038-280~Modified Winterbottom Construction Including Boundaries~2021-12-06 08:27:14.01",
            dict(
                doi="10.1021/acs.jpcc.0c07316",
                journal="The Journal of Physical Chemistry C",
                year="2020",
                title="Modified Winterbottom Construction Including Boundaries",
            ),
        ),
        (
            "Journal of Chemical Theory and Computation~2021~In Pres~Marks, L. D.~10.1021/acs.jctc.1c00630~Predictive Mixing for Density Functional Theory (and other Fixed-Point Problems)~2021-12-06 08:27:14.013",
            dict(
                doi="10.1021/acs.jctc.1c00630",
                journal="Journal of Chemical Theory and Computation",
                year="2021",
                title="Predictive Mixing for Density Functional Theory (and other Fixed-Point Problems)",
            ),
        ),
        (
            "Journal of Applied Physics~2021~129~Mizzi, Christopher A. and Marks, Laurence D.~10.1063/5.0048920~224102~The role of surfaces in flexoelectricity~2021-12-06 08:27:14.02",
            dict(
                doi="10.1063/5.0048920",
                journal="Journal of Applied Physics",
                year="2021",
                title="The role of surfaces in flexoelectricity",
            ),
        ),
        (
            "Crystals~2019~9~Paull, R. J. and Ly, T. and Mansley, Z. R. and Poeppelmeier, K. R. and Marks, L. D.~10.3390/cryst9040218~218~Controlled Two-Step Formation of Faceted Perovskite Rare-Earth Scandate Nanoparticles~2021-12-06 08:27:14.03",
            dict(
                doi="10.3390/cryst9040218",
                journal="Crystals",
                year="2019",
                title="Controlled Two-Step Formation of Faceted Perovskite Rare-Earth Scandate Nanoparticles",
            ),
        ),
        (
            "Inorg Chem~2018~57~Paull, R. J., Mansley, Z. R., Ly, T., Marks, L. D.,Poeppelmeier, K. R.~10.1021/acs.inorgchem.8b00281~4104~Synthesis of Gadolinium Scandate from a Hydroxide Hydrogel~2019-09-04 05:34:40.76",
            dict(
                doi="10.1021/acs.inorgchem.8b00281",
                journal="Inorg Chem",
                year="2018",
                title="Synthesis of Gadolinium Scandate from a Hydroxide Hydrogel",
            ),
        ),
        (
            "Crystals~2019~9~Paull, Ryan J., Ly, Tiffany, Mansley, Zachary R., Poeppelmeier, Kenneth R., Marks, Laurence D.~10.3390/cryst9040218~218~Controlled Two-Step Formation of Faceted Perovskite Rare-Earth Scandate Nanoparticles~2019-09-04 05:34:40.766",
            dict(
                doi="10.3390/cryst9040218",
                journal="Crystals",
                year="2019",
                title="Controlled Two-Step Formation of Faceted Perovskite Rare-Earth Scandate Nanoparticles",
            ),
        ),
        (
            "Inorganic Chemistry~2018~57~Ryan J. Paull, Zachary R. Mansley, Tiffany Ly, Laurence D. Marks, and Kenneth R. Poeppelmeier~10.1021/acs.inorgchem.8b00281~4104~Synthesis of Gadolinium Scandate from a Hydroxide Hydrogel~2018-06-29 11:25:00.963",
            dict(
                doi="10.1021/acs.inorgchem.8b00281",
                journal="Inorganic Chemistry",
                year="2018",
                title="Synthesis of Gadolinium Scandate from a Hydroxide Hydrogel",
            ),
        ),
        (
            "Surface Science Reports~2018~73~T. K. Andersen, D. D. Fong and L. D. Marks~10.1016/j.surfrep.2018.08.001~213~Pauling's Rules for Oxide Surfaces~2019-09-04 05:34:40.773",
            dict(
                doi="10.1016/j.surfrep.2018.08.001",
                journal="Surface Science Reports",
                year="2018",
                title="Pauling's Rules for Oxide Surfaces",
            ),
        ),
        (
            "Nano Letters~2018~18~T. Ly, J. Wen and L. D. Marks~10.1021/acs.nanolett.8b02123~5186~Kinetic Growth Regimes of Hydrothermally Synthesized Potassium Tantalate Nanoparticles~2019-09-04 05:34:40.776",
            dict(
                doi="10.1021/acs.nanolett.8b02123",
                journal="Nano Letters",
                year="2018",
                title="Kinetic Growth Regimes of Hydrothermally Synthesized Potassium Tantalate Nanoparticles",
            ),
        ),
        # Next Award
        (
            "Deep Sea Research II~2012~65~Cooper, L.W., M. Janout, K.E. Frey, R. Pirtle-Levy, M. Guarinello, J.M. Grebmeier, and J.R. Lovvorn~10.1016/j.dsr2.2012.02.002~141-162~The relationship between chlorophyll biomass, water mass variation, and sea ice melt timing in the northern Bering Sea~N~",
            dict(
                doi="10.1016/j.dsr2.2012.02.002",
                journal="Deep Sea Research II",
                year="2012",
                title="The relationship between chlorophyll biomass, water mass variation, and sea ice melt timing in the northern Bering Sea",
            ),
        ),
        (
            "Marine Ecology Progress Series~2009~393~Cui, X., J. M. Grebmeier, L. W. Cooper, J. R. Lovvorn, C. A. North, and J. M. Kolts~147~Spatial distributions of groundfish in the northern Bering Sea in relation to environmental variation~N~",
            dict(
                doi=None,
                journal="Marine Ecology Progress Series",
                year="2009",
                title="Spatial distributions of groundfish in the northern Bering Sea in relation to environmental variation",
            ),
        ),
        (
            "Marine Ecology Progress Series~2009~393~Cui, X., J. M. Grebmeier, L. W. Cooper, J. R. Lovvorn, C. A. North, and J. M. Kolts~147~Spatial distributions of groundfish in the northern Bering Sea in relation to environmental variation~N~",
            dict(
                doi=None,
                journal="Marine Ecology Progress Series",
                year="2009",
                title="Spatial distributions of groundfish in the northern Bering Sea in relation to environmental variation",
            ),
        ),
        (
            "Oceanography~2009~22~Fabry, V.J., J.B. McClintock, J.T. Mathis, and J.M. Grebmeier~160~Ocean acidification at High Latitudes: The Bellwether~N~",
            dict(
                doi=None,
                journal="Oceanography",
                year="2009",
                title="Ocean acidification at High Latitudes: The Bellwether",
            ),
        ),
        (
            "Annual Review of Marine Science~2012~4~Grebmeier, J.M.~10.114/annurev-marine-120710-100926~63~Biological community shifts in Pacifc Arctic and sub-Arctic seas~N~",
            dict(
                doi="10.114/annurev-marine-120710-100926",
                journal="Annual Review of Marine Science",
                year="2012",
                title="Biological community shifts in Pacifc Arctic and sub-Arctic seas",
            ),
        ),
        (
            "Eos Transactions of the American Geophysical Union~2010~91~Grebmeier, J.M, S.E. Moore, J.E. Overland, K.E. Frey, and R. Gradinger~161~Biological Response to Recent Pacific Arctic Sea Ice Retreats~N~",
            dict(
                doi=None,
                journal="Eos Transactions of the American Geophysical Union",
                year="2010",
                title="Biological Response to Recent Pacific Arctic Sea Ice Retreats",
            ),
        ),
        (
            "Polar Biology~2012~Heide-JÃ¸rgensen, K.L. Laidre, D. Litovka, M. Villum Jensen, J.M. Grebmeier, and B.I. Sirenko~. DOI10.1007/s00300-011-1151-6~Identifying gray whale (Eschrichtius robustus) foraging grounds along the Chukotka Peninsula, Russia, using satellite telemetry~N~",
            dict(
                doi="10.1007/s00300-011-1151-6",
                journal="Polar Biology",
                year="2012",
                title="Identifying gray whale (Eschrichtius robustus) foraging grounds along the Chukotka Peninsula, Russia, using satellite telemetry",
            ),
        ),
        (
            "Arctic~2009~63~Lovvorn, J.R.,  J.J. Wilson, D. McKay, J.H. Bump, L.W. Cooper, and J.M. Grebmeier~5~Walruses attack spectacled eiders wintering in pack ice of the Bering Sea~N~",
            dict(
                doi=None,
                journal="Arctic",
                year="2009",
                title="Walruses attack spectacled eiders wintering in pack ice of the Bering Sea",
            ),
        ),
        (
            "Arctic~2009~63~Lovvorn, J.R.,  J.J. Wilson, D. McKay, J.H. Bump, L.W. Cooper, and J.M. Grebmeier~51~Walruses attack spectacled eiders wintering in pack ice of the Bering Sea~N~",
            dict(
                doi=None,
                journal="Arctic",
                year="2009",
                title="Walruses attack spectacled eiders wintering in pack ice of the Bering Sea",
            ),
        ),
        (
            "Ecological Applications~2009~19~Lovvorn, J.R., J.M. Grebmeier, L.W. Cooper, J.K. Bump, and J.G. Richman~1596~Modeling marine protected areas for threatened eiders in a climatically shifting Bering Sea~N~",
            dict(
                doi=None,
                journal="Ecological Applications",
                year="2009",
                title="Modeling marine protected areas for threatened eiders in a climatically shifting Bering Sea",
            ),
        ),
        (
            "Ecological Applications~2009~19~Lovvorn, J.R., J.M. Grebmeier, L.W. Cooper, J.K. Bump, and J.G. Richman~1596~Modeling marine protected areas for threatened eiders in a climatically shifting Bering Sea~N~",
            dict(
                doi=None,
                journal="Ecological Applications",
                year="2009",
                title="Modeling marine protected areas for threatened eiders in a climatically shifting Bering Sea",
            ),
        ),
        (
            "Marine Biodiversity~2011~41~Piepenburg, D., P. Archambault, W.G. Ambrose Jr., A. Blanchard, B.A. Bluhm, M.L. Carroll, K.E. Conlan, M. Cusson, H.M. Feder, J.M. Grebmeier, S.C. Jewett, M. LÃ???Ã??Ã?Â©vesque, V.V. Petryashev, M.K. Sejr, B.I. Sirenko, M. Wlodarska-Kowalczuk, 2010. Towar~10.1007/s12526-010-0059-7~517~Towards a pan-Arctic inventory of the species diversity of the macro- and megabenthic fauna of the Arctic shelf seas~N~",
            dict(
                doi="10.1007/s12526-010-0059-7",
                journal="Marine Biodiversity",
                year="2011",
                title="Towards a pan-Arctic inventory of the species diversity of the macro- and megabenthic fauna of the Arctic shelf seas",
            ),
        ),
        (
            "Marine Biodiversity~2011~41~Piepenburg, D., P. Archambault, W.G. Ambrose Jr., A. Blanchard, B.A. Bluhm, M.L. Carroll, K.E. Conlan, M. Cusson, H.M. Feder, J.M. Grebmeier, S.C. Jewett, M. LÃ??Ã?Â©vesque, V.V. Petryashev, M.K. Sejr, B.I. Sirenko, M. Wlodarska-Kowalczuk, 2010. Towards a~10.1007/s12526-010-0059-7~5170~Towards a pan-Arctic inventory of the species diversity of the macro- and megabenthic fauna of the Arctic shelf seas~N~",
            dict(
                doi="10.1007/s12526-010-0059-7",
                journal="Marine Biodiversity",
                year="2011",
                title="Towards a pan-Arctic inventory of the species diversity of the macro- and megabenthic fauna of the Arctic shelf seas",
            ),
        ),
        (
            "Marine Mammal Science~2009~25~Sheffield, G., and J.M. Grebmeier~7~Pacific walrus: Differential digestion and diet~N~",
            dict(
                doi=None,
                journal="Marine Mammal Science",
                year="2009",
                title="Pacific walrus: Differential digestion and diet",
            ),
        ),
        (
            "Microbiology~2011~Zeng, Y, Z. Yang, B. Chen, J.M. Grebmeier, H. Li, Y. Yu, and T. Zheng.~10.1007/s00300-010-0947-0~High Bacterial Diversity in Marine Sediments of the Northern Bering Sea~N~",
            dict(
                doi="10.1007/s00300-010-0947-0",
                journal="Microbiology",
                year="2011",
                title="High Bacterial Diversity in Marine Sediments of the Northern Bering Sea",
            ),
        ),
        (
            "Polar Biology~2011~34~Zeng, Y, Z. Yang, B. Chen, J.M. Grebmeier, H. Li, Y.Yu, and T. Zheng~DOI 10.1007/s00300-010-0947-0~907~Phylogenetic diversity of sediment bacteria in the Northern Bering Sea~N~",
            dict(
                doi="10.1007/s00300-010-0947-0",
                journal="Polar Biology",
                year="2011",
                title="Phylogenetic diversity of sediment bacteria in the Northern Bering Sea",
            ),
        ),
        (
            "Polar Biology~2011~35~Zeng, Y., Z. Yang, J.M. Grebmeier, J. Hi, and T. Zheng~DOI 10.1007/s00300-011-1044-8~117~Culture-independent and -dependent methods to investigate the diversity of planktonic bacteria in the northern Bering Sea~N~",
            dict(
                doi="10.1007/s00300-011-1044-8",
                journal="Polar Biology",
                year="2011",
                title="Culture-independent and -dependent methods to investigate the diversity of planktonic bacteria in the northern Bering Sea",
            ),
        ),
    ]

    for ref, expected in rows:
        print(ref)
        parsed = parse_reference(ref)
        del parsed["reference"]
        assert expected == parsed


def test_extract_doi():
    doi = extract_doi("DOI 10.1007/s00300-010-0947-0")
    assert "10.1007/s00300-010-0947-0" == doi

    doi = extract_doi("DOI10.1007/s00300-010-0947-0")
    assert "10.1007/s00300-010-0947-0" == doi


def test_nsf_fetch_org_id():
    with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nsf_fetch_org_id.yaml")):
        # NSF Org ID exists
        org_id = nsf_fetch_org_id("2234213")
        assert "EAR" == org_id

        # NSF Org ID None
        org_id = nsf_fetch_org_id("0")
        assert None == org_id
